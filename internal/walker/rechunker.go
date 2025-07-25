package walker

import (
	"context"
	"fmt"
	"io"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

// scope of this file:
// rechunk file contents and rewrite tree nodes.
// rewriting snapshot is out of scope of this file.
// (do it in cmd_rechunk.go)

// type RechunkFileJob struct {
// 	srcBlobIDs restic.IDs
// dstBlobIDs restic.IDs
// node       *restic.Node
// callback   func()
// }

type BlobIDsPair struct {
	srcBlobIDs    restic.IDs
	dstBlobIDs    restic.IDs
}

type FileRechunker struct {
	// ch chan<-     restic.IDs
	srcRepo         	restic.BlobLoader
	dstRepo         	restic.BlobSaver
	dstRepoPol			chunker.Pol
	rechunkBlobsMap     map[string]BlobIDsPair
	rewriteTreeMap		map[restic.ID]restic.ID
	visitedBlobs	  	map[string]struct{}
	// cache     *bloblru.Cache
	// chnker    *chunker.Chunker
}

const blobCacheSize = 64 << 20  // same with fuse.blobCacheSize

func NewFileRechunker(srcRepo restic.BlobLoader, dstRepo restic.BlobSaver, dstRepoPol chunker.Pol) *FileRechunker {
	return &FileRechunker{
		srcRepo: 			srcRepo,
		dstRepo: 			dstRepo,
		dstRepoPol:			dstRepoPol,
		rechunkBlobsMap: 	make(map[string]BlobIDsPair),
		rewriteTreeMap:     make(map[restic.ID]restic.ID),
		visitedBlobs:	 	make(map[string]struct{}),
	}
}

func (rc *FileRechunker) RechunkData(ctx context.Context, root restic.ID) error {
	numWorkers := 2  // TODO: make it configurable
	chFile := make(chan restic.IDs, numWorkers)
	visitor := WalkVisitor{
		ProcessNode: func(_ restic.ID, _ string, node *restic.Node, nodeErr error) error {
			// skip non-file nodes
			if node.Type != restic.NodeTypeFile {
				return nil
			}

			// skip if identical file content is already registered
			ids := node.Content.String()
			if _, ok := rc.visitedBlobs[ids]; ok {
				return nil
			}
			rc.visitedBlobs[ids] = struct{}{}

			select {
			case chFile <- node.Content:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}
	blobCache := bloblru.New(blobCacheSize)

	// create rechunk workers
	wgUp, wgUpCtx := errgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		wgUp.Go(func() error {
			chnker := chunker.New(nil, rc.dstRepoPol)
			chReuseBuf := make(chan []byte, 1)
			wg, wgCtx := errgroup.WithContext(wgUpCtx)

			for {
				var srcBlobs restic.IDs
				var ok bool
				select {
				case <-wgUpCtx.Done():  // context cancelled
					return wgUpCtx.Err()
				case srcBlobs, ok = <-chFile:
					if !ok {  // chan closed and drained
						return nil
					}
				}
				dstBlobs := restic.IDs{}
				
				// run downloader/iopipe/chunker/uploader goroutine
				// downloader
				chDownload := make(chan []byte)
				wg.Go(func() error {
					for _, blobID := range srcBlobs {
						buf, err := blobCache.GetOrCompute(blobID, func() ([]byte, error) {
							return rc.srcRepo.LoadBlob(wgCtx, restic.DataBlob, blobID, nil)
						})
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

				// iopipe
				r, w := io.Pipe()
				wg.Go(func() error {
					for {
						var buf []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case buf, ok = <- chDownload:
							if !ok {  // EOF
								w.Close()
								return nil
							}
						}

						_, err := w.Write(buf)
						if err != nil {
							return err
						}
					}
				})
				
				// chunker
				chnker.Reset(r, rc.dstRepoPol)
				chUpload := make(chan []byte)
				wg.Go(func() error {
					for {
						var buf []byte
						select {
						case buf = <-chReuseBuf:
						default:
							buf = make([]byte, chunker.MaxSize)
						}

						chunk, err := chnker.Next(buf)
						if err == io.EOF {
							close(chUpload)
							return nil
						}
						if err != nil {
							return err
						}

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chUpload <- chunk.Data:
						}
					}
				})

				// uploader
				wg.Go(func() error {
					for {
						var blobData []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case blobData, ok = <-chUpload:
							if !ok {  // EOF
								return nil
							}
						}

						blobID, _, _, err := rc.dstRepo.SaveBlob(ctx, restic.DataBlob, blobData, restic.ID{}, false)
						if err != nil {
							return err
						}

						select {
						case chReuseBuf <- blobData:
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
				rc.rechunkBlobsMap[srcBlobs.String()] = BlobIDsPair{
					srcBlobIDs: srcBlobs,
					dstBlobIDs: dstBlobs,
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
	return wgUp.Wait()
}

func (rc *FileRechunker) rewriteNode(node *restic.Node) error {
	if node.Type != restic.NodeTypeFile {
		return nil
	}

	ids := node.Content.String()
	blobsPair, ok := rc.rechunkBlobsMap[ids]
	if !ok {  // critical bug!
		return fmt.Errorf("Can't find from rechunkBlobsMap: %v", ids)
	}
	node.Content = blobsPair.dstBlobIDs
	return nil
}

func (rc *FileRechunker) RewriteTree(ctx context.Context, nodeID restic.ID) (newNodeID restic.ID, err error) {
	// check if tree was already changed
	newID, ok := rc.rewriteTreeMap[nodeID]
	if ok {
		return newID, nil
	}

	// a nil nodeID will lead to a load error
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
		// treat nil as null id
		var subtree restic.ID
		if node.Subtree != nil {
			subtree = *node.Subtree
		}
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
