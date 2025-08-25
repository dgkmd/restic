package walker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
	"golang.org/x/sync/errgroup"
)

type hashType = [32]byte

type Rechunker struct {
	srcRepo             restic.BlobLoader
	dstRepo             restic.BlobSaver
	dstRepoPol          chunker.Pol
	rechunkBlobsMap     map[hashType]restic.IDs
	rechunkBlobsMapLock sync.Mutex
	rewriteTreeMap      map[restic.ID]restic.ID
	visitedBlobs        map[hashType]struct{}
}

func NewRechunker(srcRepo restic.BlobLoader, dstRepo restic.BlobSaver, dstRepoPol chunker.Pol) *Rechunker {
	return &Rechunker{
		srcRepo:         srcRepo,
		dstRepo:         dstRepo,
		dstRepoPol:      dstRepoPol,
		rechunkBlobsMap: make(map[hashType]restic.IDs),
		rewriteTreeMap:  make(map[restic.ID]restic.ID),
		visitedBlobs:    make(map[hashType]struct{}),
	}
}

func (rc *Rechunker) RechunkData(ctx context.Context, root restic.ID, p *progress.Counter) error {
	numWorkers := runtime.GOMAXPROCS(0)
	chFile := make(chan restic.IDs, numWorkers)
	bufferPool := make(chan []byte, 4*numWorkers)
	visitor := WalkVisitor{
		ProcessNode: func(_ restic.ID, _ string, node *restic.Node, nodeErr error) error {
			// skip root node (node == nil)
			if node == nil {
				return nil
			}
			// skip non-file nodes
			if node.Type != restic.NodeTypeFile {
				return nil
			}

			// skip if identical file content has already been visited
			hashval := hashOfIDs(node.Content)
			if _, ok := rc.visitedBlobs[hashval]; ok {
				if p != nil {
					p.Add(1)
				}
				return nil
			}
			rc.visitedBlobs[hashval] = struct{}{}

			select {
			case chFile <- node.Content:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}

	// create rechunk workers
	wgUp, wgUpCtx := errgroup.WithContext(ctx)
	for range numWorkers {
		wgUp.Go(func() error {
			chnker := chunker.New(nil, rc.dstRepoPol)

			for {
				var srcBlobs restic.IDs
				var ok bool
				select {
				case <-wgUpCtx.Done():
					return wgUpCtx.Err()
				case srcBlobs, ok = <-chFile:
					if !ok { // all files visited and chan closed
						return nil
					}
				}
				dstBlobs := restic.IDs{}
				wg, wgCtx := errgroup.WithContext(wgUpCtx)

				// run downloader/iopipe/chunker/uploader Goroutines per each file
				// downloader: load original chunks one by one from source repo
				chDownload := make(chan []byte)
				wg.Go(func() error {
					for _, blobID := range srcBlobs {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							debug.Log("make buffer (1)")
							buf = make([]byte, chunker.MaxSize)
						}
						buf, err := rc.srcRepo.LoadBlob(wgCtx, restic.DataBlob, blobID, buf)
						if err != nil {
							return err
						}

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chDownload <- buf:
						}
					}
					close(chDownload)
					return nil
				})

				// iopipe: convert chunks into io.Reader stream
				r, w := io.Pipe()
				wg.Go(func() error {
					for {
						var buf []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							w.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case buf, ok = <-chDownload:
							if !ok { // EOF
								w.Close()
								return nil
							}
						}

						_, err := w.Write(buf)
						if err != nil {
							w.CloseWithError(err)
							return err
						}
						select {
						case bufferPool <- buf:
						default:
						}
					}

				})

				// chunker: rechunk filestream with destination repo's chunking parameter
				chnker.Reset(r, rc.dstRepoPol)
				chUpload := make(chan []byte)
				wg.Go(func() error {
					for {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							debug.Log("make buffer (2)")
							buf = make([]byte, chunker.MaxSize)
						}

						chunk, err := chnker.Next(buf)
						if err == io.EOF {
							select {
							case bufferPool <- buf:
							default:
							}
							close(chUpload)
							return nil
						}
						if err != nil {
							r.CloseWithError(err)
							return err
						}

						select {
						case <-wgCtx.Done():
							r.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case chUpload <- chunk.Data:
						}
					}
				})

				// uploader: save rechunked blobs into destination repo
				wg.Go(func() error {
					for {
						var blobData []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case blobData, ok = <-chUpload:
							if !ok { // EOF
								return nil
							}
						}

						blobID, _, _, err := rc.dstRepo.SaveBlob(ctx, restic.DataBlob, blobData, restic.ID{}, false)
						if err != nil {
							return err
						}

						select {
						case bufferPool <- blobData:
						default:
						}
						dstBlobs = append(dstBlobs, blobID)
					}
				})

				err := wg.Wait()
				if err != nil {
					return err
				}

				// register to rechunkMap
				rc.rechunkBlobsMapLock.Lock()
				rc.rechunkBlobsMap[hashOfIDs(srcBlobs)] = dstBlobs
				rc.rechunkBlobsMapLock.Unlock()

				if p != nil {
					p.Add(1)
				}
			}
		})
	}

	// call Walk(): register jobs to ch
	err := Walk(ctx, rc.srcRepo, root, visitor)
	close(chFile)
	if err != nil {
		return err
	}

	// wait for rechunk jobs to finish
	return wgUp.Wait()
}

func (rc *Rechunker) rewriteNode(node *restic.Node) error {
	if node.Type != restic.NodeTypeFile {
		return nil
	}

	hashval := hashOfIDs(node.Content)
	dstBlobs, ok := rc.rechunkBlobsMap[hashval]
	if !ok {
		return fmt.Errorf("can't find from rechunkBlobsMap: %v", node.Content.String())
	}
	node.Content = dstBlobs
	return nil
}

func (rc *Rechunker) RewriteTree(ctx context.Context, nodeID restic.ID) (restic.ID, error) {
	// check if the identical tree has already been processed
	newID, ok := rc.rewriteTreeMap[nodeID]
	if ok {
		return newID, nil
	}

	curTree, err := restic.LoadTree(ctx, rc.srcRepo, nodeID)
	if err != nil {
		return restic.ID{}, err
	}

	tb := restic.NewTreeJSONBuilder()
	for _, node := range curTree.Nodes {
		if ctx.Err() != nil {
			return restic.ID{}, ctx.Err()
		}

		err = rc.rewriteNode(node)
		if err != nil {
			return restic.ID{}, err
		}

		if node.Type != restic.NodeTypeDir {
			err = tb.AddNode(node)
			if err != nil {
				return restic.ID{}, err
			}
			continue
		}

		subtree := *node.Subtree
		newID, err := rc.RewriteTree(ctx, subtree)
		if err != nil {
			return restic.ID{}, err
		}
		node.Subtree = &newID
		err = tb.AddNode(node)
		if err != nil {
			return restic.ID{}, err
		}
	}

	tree, err := tb.Finalize()
	if err != nil {
		return restic.ID{}, err
	}

	// Save new tree
	newTreeID, _, _, err := rc.dstRepo.SaveBlob(ctx, restic.TreeBlob, tree, restic.ID{}, false)
	rc.rewriteTreeMap[nodeID] = newTreeID
	return newTreeID, err
}

func hashOfIDs(ids restic.IDs) hashType {
	c := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		c = append(c, id[:]...)
	}
	return sha256.Sum256(c)
}
