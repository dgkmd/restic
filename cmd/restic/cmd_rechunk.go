// plan: open repos -> 

package main

import (
	"context"
	"fmt"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newRechunkCommand() *cobra.Command {
	var opts RechunkOptions
	cmd := &cobra.Command{
		Use:   "rechunk [flags] [snapshotID ...]",
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRechunk(cmd.Context(), opts, globalOptions, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// RechunkOptions bundles all options for the copy command.
type RechunkOptions struct {
	secondaryRepoOptions
	restic.SnapshotFilter
}

func (opts *RechunkOptions) AddFlags(f *pflag.FlagSet) {
	opts.secondaryRepoOptions.AddFlags(f, "destination", "to copy snapshots from")
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
}

type idMap map[restic.ID]restic.ID

type rechunkFileJob struct {
	srcBlobs restic.IDs
	dstBlobs restic.IDs
	node     *restic.Node
}

type Rechunker struct {}

func (rch *Rechunker) commit() error {
	// after rechunking and upload, write dstBlobs to node.Content and notify to tree rewriter
	return nil
}

func runRechunk(ctx context.Context, opts RechunkOptions, gopts GlobalOptions, args []string) error {
	secondaryGopts, isFromRepo, err := fillSecondaryGlobalOpts(ctx, opts.secondaryRepoOptions, gopts, "destination")
	if err != nil {
		return err
	}
	if isFromRepo {
		// swap global options, if the secondary repo was set via from-repo
		gopts, secondaryGopts = secondaryGopts, gopts
	}

	ctx, srcRepo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock)
	if err != nil {
		return err
	}
	defer unlock()

	ctx, dstRepo, unlock, err := openWithAppendLock(ctx, secondaryGopts, false)
	if err != nil {
		return err
	}
	defer unlock()

	srcSnapshotLister, err := restic.MemorizeList(ctx, srcRepo, restic.SnapshotFile)
	if err != nil {
		return err
	}

	dstSnapshotLister, err := restic.MemorizeList(ctx, dstRepo, restic.SnapshotFile)
	if err != nil {
		return err
	}

	debug.Log("Loading source index")
	bar := newIndexProgress(gopts.Quiet, gopts.JSON)
	if err := srcRepo.LoadIndex(ctx, bar); err != nil {
		return err
	}
	bar = newIndexProgress(gopts.Quiet, gopts.JSON)
	debug.Log("Loading destination index")
	if err := dstRepo.LoadIndex(ctx, bar); err != nil {
		return err
	}

	dstSnapshotByOriginal := make(map[restic.ID][]*restic.Snapshot)
	for sn := range FindFilteredSnapshots(ctx, dstSnapshotLister, dstRepo, &opts.SnapshotFilter, nil) {
		if sn.Original != nil && !sn.Original.IsNull() {
			dstSnapshotByOriginal[*sn.Original] = append(dstSnapshotByOriginal[*sn.Original], sn)
		}
		// also consider identical snapshot copies
		dstSnapshotByOriginal[*sn.ID()] = append(dstSnapshotByOriginal[*sn.ID()], sn)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// remember already processed trees across all snapshots
	visitedTrees := make(idMap)

	for sn := range FindFilteredSnapshots(ctx, srcSnapshotLister, srcRepo, &opts.SnapshotFilter, args) {
		Verbosef("\n%v\n", sn)
		Verbosef("  rechunk copy started, this may take a while...\n")
		if err := rechunkTree(ctx, srcRepo, dstRepo, visitedTrees, *sn.Tree, gopts.Quiet); err != nil {
			return err
		}
		debug.Log("tree copied")

		// save snapshot
		sn.Parent = nil // Parent does not have relevance in the new repo.
		// Use Original as a persistent snapshot ID
		if sn.Original == nil {
			sn.Original = sn.ID()
		}
		newID, err := restic.SaveSnapshot(ctx, dstRepo, sn)
		if err != nil {
			return err
		}
		Verbosef("snapshot %s saved\n", newID.Str())
	}

	return ctx.Err()
}

func rechunkTree(ctx context.Context, srcRepo restic.Repository, dstRepo restic.Repository,
	visitedTrees idMap, rootTreeID restic.ID, quiet bool) error {

	// refer to and modify rewriter's logic

	return nil
}

func copyTreeRechunk(ctx context.Context, srcRepo restic.Repository, dstRepo restic.Repository,
	visitedTrees restic.IDSet, rootTreeID restic.ID, quiet bool) error {

	wg, wgCtx := errgroup.WithContext(ctx)

	treeStream := restic.StreamTrees(wgCtx, wg, srcRepo, restic.IDs{rootTreeID}, func(treeID restic.ID) bool {
		visited := visitedTrees.Has(treeID)
		visitedTrees.Insert(treeID)
		return visited
	}, nil)

	copyBlobs := restic.NewBlobSet()
	packList := restic.NewIDSet()

	enqueue := func(h restic.BlobHandle) {
		pb := srcRepo.LookupBlob(h.Type, h.ID)
		copyBlobs.Insert(h)
		for _, p := range pb {
			packList.Insert(p.PackID)
		}
	}

	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return fmt.Errorf("LoadTree(%v) returned error %v", tree.ID.Str(), tree.Error)
			}

			// Do we already have this tree blob?
			treeHandle := restic.BlobHandle{ID: tree.ID, Type: restic.TreeBlob}
			if _, ok := dstRepo.LookupBlobSize(treeHandle.Type, treeHandle.ID); !ok {
				// copy raw tree bytes to avoid problems if the serialization changes
				enqueue(treeHandle)
			}

			for _, entry := range tree.Nodes {
				// Recursion into directories is handled by StreamTrees
				// Copy the blobs for this file.
				for _, blobID := range entry.Content {
					h := restic.BlobHandle{Type: restic.DataBlob, ID: blobID}
					if _, ok := dstRepo.LookupBlobSize(h.Type, h.ID); !ok {
						enqueue(h)
					}
				}
			}
		}
		return nil
	})
	err := wg.Wait()
	if err != nil {
		return err
	}

	bar := newProgressMax(!quiet, uint64(len(packList)), "packs copied")
	_, err = repository.Repack(
		ctx,
		srcRepo,
		dstRepo,
		packList,
		copyBlobs,
		bar,
		func(msg string, args ...interface{}) { fmt.Printf(msg+"\n", args...) },
	)
	bar.Done()
	if err != nil {
		return errors.Fatal(err.Error())
	}
	return nil
}
