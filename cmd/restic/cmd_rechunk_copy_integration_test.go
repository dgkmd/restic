package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	rtest "github.com/restic/restic/internal/test"
)

func testRunRechunkCopy(t testing.TB, srcGopts GlobalOptions, dstGopts GlobalOptions) {
	gopts := srcGopts
	gopts.Repo = dstGopts.Repo
	gopts.password = dstGopts.password
	gopts.InsecureNoPassword = dstGopts.InsecureNoPassword
	copyOpts := RechunkCopyOptions{
		secondaryRepoOptions: secondaryRepoOptions{
			Repo:               srcGopts.Repo,
			password:           srcGopts.password,
			InsecureNoPassword: srcGopts.InsecureNoPassword,
		},
	}

	rtest.OK(t, runRechunkCopy(context.TODO(), copyOpts, gopts, nil))
}

func TestRechunkCopy(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env2, cleanup2 := withTestEnvironment(t)
	defer cleanup2()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	testRunCheck(t, env.gopts)

	testRunInit(t, env2.gopts)
	testRunRechunkCopy(t, env.gopts, env2.gopts)

	snapshotIDs := testListSnapshots(t, env.gopts, 3)
	copiedSnapshotIDs := testListSnapshots(t, env2.gopts, 3)

	// Check that the copies size seems reasonable
	stat := dirStats(env.repo)
	stat2 := dirStats(env2.repo)
	sizeDiff := int64(stat.size) - int64(stat2.size)
	if sizeDiff < 0 {
		sizeDiff = -sizeDiff
	}
	rtest.Assert(t, sizeDiff < int64(stat.size)/50, "expected less than 2%% size difference: %v vs. %v",
		stat.size, stat2.size)

	// Check integrity of the copy
	testRunCheck(t, env2.gopts)

	// Check that the copied snapshots have the same tree contents as the old ones (= identical tree hash)
	origRestores := make(map[string]struct{})
	for i, snapshotID := range snapshotIDs {
		restoredir := filepath.Join(env.base, fmt.Sprintf("restore%d", i))
		origRestores[restoredir] = struct{}{}
		testRunRestore(t, env.gopts, restoredir, snapshotID.String())
	}
	for i, snapshotID := range copiedSnapshotIDs {
		restoredir := filepath.Join(env2.base, fmt.Sprintf("restore%d", i))
		testRunRestore(t, env2.gopts, restoredir, snapshotID.String())
		foundMatch := false
		for cmpdir := range origRestores {
			diff := directoriesContentsDiff(restoredir, cmpdir)
			if diff == "" {
				delete(origRestores, cmpdir)
				foundMatch = true
			}
		}

		rtest.Assert(t, foundMatch, "found no counterpart for snapshot %v", snapshotID)
	}

	rtest.Assert(t, len(origRestores) == 0, "found not copied snapshots")
}
