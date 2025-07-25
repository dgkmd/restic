package walker

import (
	"context"
	"errors"
	"fmt"

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
	ch := make(chan restic.IDs)
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
			case ch <- node.Content:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}

	// create rechunk workers
	numWorkers := 4  // TODO: make it configurable
	wg, wgCtx := errgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		wg.Go(func() error {
			var srcBlobs restic.IDs
			var dstBlobs restic.IDs
			// where should I put chunkers?
			// is it better to place it globally for performance?
			chnker := chunker.New(nil, rc.dstRepoPol)
			for {
				select {
				case <-wgCtx.Done():  // context cancelled
					return wgCtx.Err()
				case b, ok := <-ch:
					if !ok {  // chan closed and drained
						return nil
					}
					srcBlobs = b
				}
				// rechunk srcBlobs to dstBlobs
				// seems I need to implement buffer manually...
				// reference implementation: cmd_dump.go, file_saver.go

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
	close(ch)
	if err != nil {
		return err
	}
	return wg.Wait()
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
