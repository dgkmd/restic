package main

import (
	"context"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
	"golang.org/x/sync/errgroup"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Reference: cmd_copy.go (v0.18.0)

func newRechunkCopyCommand() *cobra.Command {
	var opts RechunkCopyOptions
	cmd := &cobra.Command{
		Use:   "rechunk-copy [flags] [snapshotID ...]",
		Short: "Rechunk-copy snapshots from one repository to another",
		Long: `
The "rechunk-copy" command rechunk-copies one or more snapshots from one repository to another.

Data blobs stored in the destination repo are rechunked, and tree blobs in the destination repo are also updated accordingly.

NOTE: This command has largely different internal mechanism from "copy" command,
due to restic's content defined chunking (CDC) design. Note that "rechunk-copy"
may consume significantly more bandwidth during the process compared to "copy", 
and may also need significantly more time to finish.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
		`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRechunkCopy(cmd.Context(), opts, globalOptions, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// RechunkCopyOptions bundles all options for the rechunk-copy command.
type RechunkCopyOptions struct {
	secondaryRepoOptions
	restic.SnapshotFilter
	RechunkTags restic.TagLists
}

func (opts *RechunkCopyOptions) AddFlags(f *pflag.FlagSet) {
	opts.secondaryRepoOptions.AddFlags(f, "destination", "to copy snapshots from")
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
	f.Var(&opts.RechunkTags, "rechunk-tag", "add `tags` for the copied snapshots in the format `tag[,tag,...]` (can be specified multiple times)")
}

func runRechunkCopy(ctx context.Context, opts RechunkCopyOptions, gopts GlobalOptions, args []string) error {
	if !feature.Flag.Enabled(feature.RechunkCopy) {
		return errors.Fatal("rechunk-copy feature flag is not set. Currently, rechunk-copy is alpha feature (disabled by default).")
	}

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

	rechunker := walker.NewRechunker(srcRepo, dstRepo)
	rootTrees := []restic.ID{}

	// first pass: gather all root trees of snapshots for rechunking
	for sn := range FindFilteredSnapshots(ctx, srcSnapshotLister, srcRepo, &opts.SnapshotFilter, args) {
		// check whether the destination has a snapshot with the same persistent ID which has similar snapshot fields
		srcOriginal := *sn.ID()
		if sn.Original != nil {
			srcOriginal = *sn.Original
		}

		if originalSns, ok := dstSnapshotByOriginal[srcOriginal]; ok {
			isCopy := false
			for _, originalSn := range originalSns {
				if similarSnapshots(originalSn, sn) {
					Verboseff("\n%v\n", sn)
					Verboseff("skipping source snapshot %s, was already copied to snapshot %s\n", sn.ID().Str(), originalSn.ID().Str())
					isCopy = true
					break
				}
			}
			if isCopy {
				continue
			}
		}

		rootTrees = append(rootTrees, *sn.Tree)
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	dstRepo.StartPackUploader(wgCtx, wg)
	if err = runRechunk(ctx, rootTrees, rechunker, gopts.Quiet); err != nil {
		return err
	}

	// second pass: rewrite trees
	Verbosef("Rewriting trees...\n")
	for sn := range FindFilteredSnapshots(ctx, srcSnapshotLister, srcRepo, &opts.SnapshotFilter, args) {
		// check whether the destination has a snapshot with the same persistent ID which has similar snapshot fields
		srcOriginal := *sn.ID()
		if sn.Original != nil {
			srcOriginal = *sn.Original
		}

		if originalSns, ok := dstSnapshotByOriginal[srcOriginal]; ok {
			isCopy := false
			for _, originalSn := range originalSns {
				if similarSnapshots(originalSn, sn) {
					Verboseff("\n%v\n", sn)
					Verboseff("skipping source snapshot %s, was already copied to snapshot %s\n", sn.ID().Str(), originalSn.ID().Str())
					isCopy = true
					break
				}
			}
			if isCopy {
				continue
			}
		}

		_, err := rechunker.RewriteTree(ctx, *sn.Tree)
		if err != nil {
			return err
		}
	}

	if err = dstRepo.Flush(wgCtx); err != nil {
		return err
	}

	Verbosef("Rewriting done.\n\n")

	// third pass: write snapshots
	for sn := range FindFilteredSnapshots(ctx, srcSnapshotLister, srcRepo, &opts.SnapshotFilter, args) {
		// check whether the destination has a snapshot with the same persistent ID which has similar snapshot fields
		srcOriginal := *sn.ID()
		if sn.Original != nil {
			srcOriginal = *sn.Original
		}

		if originalSns, ok := dstSnapshotByOriginal[srcOriginal]; ok {
			isCopy := false
			for _, originalSn := range originalSns {
				if similarSnapshots(originalSn, sn) {
					Verboseff("\n%v\n", sn)
					Verboseff("skipping source snapshot %s, was already copied to snapshot %s\n", sn.ID().Str(), originalSn.ID().Str())
					isCopy = true
					break
				}
			}
			if isCopy {
				continue
			}
		}

		// save snapshot
		sn.Parent = nil // Parent does not have relevance in the new repo.
		// Use Original as a persistent snapshot ID
		if sn.Original == nil {
			sn.Original = sn.ID()
		}

		newTreeID, err := rechunker.GetRewrittenTree(*sn.Tree)
		if err != nil {
			return err
		}
		// change Tree field to new one
		sn.Tree = &newTreeID
		// add tags if provided
		sn.AddTags(opts.RechunkTags.Flatten())
		newID, err := restic.SaveSnapshot(ctx, dstRepo, sn)
		if err != nil {
			return err
		}
		Verbosef("snapshot %s saved\n", newID.Str())
	}
	return ctx.Err()
}

func runRechunk(ctx context.Context, roots []restic.ID, rechunker *walker.Rechunker, quiet bool) error {
	Verbosef("Rechunk scheduling start...\n")
	err := rechunker.Plan(ctx, roots)
	if err != nil {
		return err
	}
	Verbosef("Scheduling Done. Rechunking data...\n")

	bar := newProgressMax(!quiet, uint64(rechunker.NumFilesToProcess()), "files rechunked")
	err = rechunker.RechunkData(ctx, bar)
	if err != nil {
		return err
	}
	bar.Done()

	Verbosef("Rechunking done.\n\n")

	return nil
}
