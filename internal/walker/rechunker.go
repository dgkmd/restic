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
	srcRepo    restic.Repository
	dstRepo    restic.Repository
	pol        chunker.Pol
	blobLoader restic.BlobLoader
	blobSaver  restic.BlobSaver

	srcFilesList      []restic.IDs
	blobRequires      map[restic.ID]uint
	srcPackToBlobsMap map[restic.ID][]restic.Blob
	srcBlobToPackMap  map[restic.ID]restic.ID
	rechunkMap        map[hashType]restic.IDs
	rechunkMapLock    sync.Mutex
	cache             *RechunkBlobCache
	chFile            chan restic.IDs
	rewriteTreeMap    map[restic.ID]restic.ID
}

func NewRechunker(srcRepo restic.Repository, dstRepo restic.Repository) *Rechunker {
	return &Rechunker{
		srcRepo:    srcRepo,
		dstRepo:    dstRepo,
		pol:        dstRepo.Config().ChunkerPolynomial,
		blobLoader: srcRepo,
		blobSaver:  dstRepo,

		srcFilesList:      []restic.IDs{},
		blobRequires:      map[restic.ID]uint{},
		srcPackToBlobsMap: map[restic.ID][]restic.Blob{},
		srcBlobToPackMap:  map[restic.ID]restic.ID{},
		rechunkMap:        map[hashType]restic.IDs{},
		cache:             NewRechunkBlobCache(),
		chFile:            make(chan restic.IDs),
		rewriteTreeMap:    map[restic.ID]restic.ID{},
	}
}

func (rc *Rechunker) Schedule(ctx context.Context, roots []restic.ID) error {
	visitedFiles := map[hashType]struct{}{}
	visitedTrees := restic.IDSet{}

	wg, wgCtx := errgroup.WithContext(ctx)
	treeStream := restic.StreamTrees(wgCtx, wg, rc.srcRepo, roots, func(id restic.ID) bool {
		visited := visitedTrees.Has(id)
		visitedTrees.Insert(id)
		return visited
	}, nil)

	// gather all unique file Contents under trees
	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return tree.Error
			}

			for _, node := range tree.Nodes {
				if node.Type == restic.NodeTypeFile {
					hashval := hashOfIDs(node.Content)
					if _, ok := visitedFiles[hashval]; ok {
						continue
					}
					visitedFiles[hashval] = struct{}{}

					rc.srcFilesList = append(rc.srcFilesList, node.Content)
				}
			}
		}
		return nil
	})
	err := wg.Wait()
	if err != nil {
		return err
	}

	// compute how many time each blob is needed
	for _, blobs := range rc.srcFilesList {
		for _, blob := range blobs {
			rc.blobRequires[blob]++
		}
	}

	// build pack<->blob map
	for blob := range rc.blobRequires {
		packs := rc.srcRepo.LookupBlob(restic.DataBlob, blob)
		if len(packs) == 0 {
			return fmt.Errorf("can't find blob from source repo: %v", blob)
		}
		pb := packs[0]

		// pack-to-blobs map
		rc.srcPackToBlobsMap[pb.PackID] = append(rc.srcPackToBlobsMap[pb.PackID], pb.Blob)

		// blob-to-pack map
		rc.srcBlobToPackMap[pb.ID] = pb.PackID
	}

	// divide into small files / large files (TODO)

	// determine the order of files (TODO?)

	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, p *progress.Counter) error {
	numWorkers := max(runtime.GOMAXPROCS(0)-1, 1)
	bufferPool := make(chan []byte, 4*numWorkers)
	wgMgr, wgMgrCtx := errgroup.WithContext(ctx)
	wgWkr, wgWkrCtx := errgroup.WithContext(ctx)

	// prepare BlobCache
	rc.cache.Start(wgMgrCtx, wgMgr, rc.srcBlobToPackMap, func(packID restic.ID) (BlobData, error) {
		blobData := BlobData{}
		err := rc.srcRepo.LoadBlobsFromPack(wgMgrCtx, packID, rc.srcPackToBlobsMap[packID],
			func(blob restic.BlobHandle, buf []byte, err error) error {
				if err != nil {
					return err
				}
				newBuf := make([]byte, len(buf))
				copy(newBuf, buf)
				blobData[blob.ID] = newBuf

				return nil
			})
		if err != nil {
			return BlobData{}, err
		}
		return blobData, nil
	})

	// worklist feeder
	// TODO: implement priority queue for 'all ready' files
	wgMgr.Go(func() error {
		for _, file := range rc.srcFilesList {
			select {
			case <-wgMgrCtx.Done():
				return wgMgrCtx.Err()
			case rc.chFile <- file:
			}
		}
		close(rc.chFile)
		return nil
	})

	// create rechunk workers
	for range numWorkers {
		wgWkr.Go(func() error {
			chnker := chunker.New(nil, rc.pol)

			for {
				var srcBlobs restic.IDs
				var ok bool
				select {
				case <-wgWkrCtx.Done():
					return wgWkrCtx.Err()
				case srcBlobs, ok = <-rc.chFile:
					if !ok { // all files finished and chan closed
						return nil
					}
				}
				dstBlobs := restic.IDs{}
				wg, wgCtx := errgroup.WithContext(wgWkrCtx)

				// run loader/iopipe/chunker/saver Goroutines per each file
				// loader: load original chunks one by one from source repo
				chFront := make(chan []byte)
				wg.Go(func() error {
					for _, blobID := range srcBlobs {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							debug.Log("make buffer (1)")
							buf = make([]byte, chunker.MaxSize)
						}

						blob, ch := rc.cache.Get(wgCtx, wg, blobID, buf)
						if blob == nil { // wait for blob to be downloaded
							select {
							case <-wgCtx.Done():
								return wgCtx.Err()
							case blob = <-ch:
							}
						}

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chFront <- blob:
						}
					}
					close(chFront)
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
						case buf, ok = <-chFront:
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
				chnker.Reset(r, rc.pol)
				chBack := make(chan []byte)
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
							close(chBack)
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
						case chBack <- chunk.Data:
						}
					}
				})

				// saver: save rechunked blobs into destination repo
				wg.Go(func() error {
					for {
						var blobData []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case blobData, ok = <-chBack:
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
				hashval := hashOfIDs(srcBlobs)
				rc.rechunkMapLock.Lock()
				rc.rechunkMap[hashval] = dstBlobs
				rc.rechunkMapLock.Unlock()

				if p != nil {
					p.Add(1)
				}
			}
		})
	}

	// wait for rechunk workers to finish
	err := wgWkr.Wait()
	if err != nil {
		return err
	}
	// finish manager jobs
	rc.cache.Stop()
	err = wgMgr.Wait()
	return err
}

func (rc *Rechunker) rewriteNode(node *restic.Node) error {
	if node.Type != restic.NodeTypeFile {
		return nil
	}

	hashval := hashOfIDs(node.Content)
	dstBlobs, ok := rc.rechunkMap[hashval]
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

	curTree, err := restic.LoadTree(ctx, rc.blobLoader, nodeID)
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
	newTreeID, _, _, err := rc.blobSaver.SaveBlob(ctx, restic.TreeBlob, tree, restic.ID{}, false)
	rc.rewriteTreeMap[nodeID] = newTreeID
	return newTreeID, err
}

func (rc *Rechunker) NumFiles() int {
	return len(rc.srcFilesList)
}

func (rc *Rechunker) GetRewrittenTree(originalTree restic.ID) (restic.ID, error) {
	newID, ok := rc.rewriteTreeMap[originalTree]
	if !ok {
		return restic.ID{}, fmt.Errorf("rewritten tree does not exist for original tree %v", originalTree)
	}
	return newID, nil
}

func hashOfIDs(ids restic.IDs) hashType {
	c := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		c = append(c, id[:]...)
	}
	return sha256.Sum256(c)
}
