package rechunker

import (
	"context"
	"fmt"
	"testing"

	"github.com/restic/chunker"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/walker"
)

// prepareData prepares random data for rechunker test.
func prepareData(t *testing.T) string {
	tempdir := rtest.TempDir(t)
	repo := archiver.TestDir{
		"0": archiver.TestFile{Content: ""},
		"1": archiver.TestFile{Content: string(rtest.Random(1, 10_000))},
		"2": archiver.TestFile{Content: string(rtest.Random(4, 10_000_000))},
		"3": archiver.TestFile{Content: string(rtest.Random(5, 100_000_000))},
	}
	archiver.TestCreateFiles(t, tempdir, repo)

	return tempdir
}

func gatherFileContentsByPath(t *testing.T, repo restic.BlobLoader, root restic.ID) map[string]restic.IDs {
	t.Helper()

	record := map[string]restic.IDs{}
	err := walker.Walk(t.Context(), repo, root, walker.WalkVisitor{
		ProcessNode: func(parentTreeID restic.ID, path string, node *data.Node, nodeErr error) (err error) {
			if node != nil && node.Type == data.NodeTypeFile {
				record[path] = node.Content
			}
			return nodeErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return record
}

func buildRechunkMapByMatchingPath(t *testing.T, srcList, dstList map[string]restic.IDs) map[restic.ID]restic.IDs {
	t.Helper()

	rechunkMap := map[restic.ID]restic.IDs{}

	for k, v := range srcList {
		if _, ok := dstList[k]; !ok {
			t.Fatalf("%v expected in dstList, but not found", k)
		}
		rechunkMap[HashOfIDs(v)] = dstList[k]
	}

	return rechunkMap
}

func TestRechunk(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// generate reandom polynomials
	srcChunkerParam, _ := chunker.RandomPolynomial()
	dstChunkerParam, _ := chunker.RandomPolynomial()

	// prepare test data
	tempdir := prepareData(t)

	// prepare repositories
	srcRepo := TestRepositoryWithPol(t, srcChunkerParam)
	dstWantsRepo := TestRepositoryWithPol(t, dstChunkerParam)
	dstTestsRepo := TestRepositoryWithPol(t, dstChunkerParam)

	srcSn := archiver.TestSnapshot(t, srcRepo, tempdir, nil)
	dstWantsSn := archiver.TestSnapshot(t, dstWantsRepo, tempdir, nil)

	srcList := gatherFileContentsByPath(t, srcRepo, *srcSn.Tree)
	dstWantsList := gatherFileContentsByPath(t, dstWantsRepo, *dstWantsSn.Tree)
	wantedRechunkMap := buildRechunkMapByMatchingPath(t, srcList, dstWantsList)

	// run rechunk copy
	rechunker := NewRechunker(Config{
		CacheSize: 4 * (1 << 30),
		Pol:       dstChunkerParam,
	})

	err := rechunker.Plan(ctx, srcRepo, restic.IDs{*srcSn.Tree})
	if err != nil {
		t.Fatal(err)
	}

	err = rechunker.Rechunk(ctx, srcRepo, dstTestsRepo, nil)
	if err != nil {
		t.Fatal(err)
	}

	// compare dstTestsRepo (rechunker result) vs dstWantsRepo (reference result)
	// 1) check if all expected data blobs are stored
	inCtx, stop := context.WithCancelCause(ctx)
	err = dstWantsRepo.ListBlobs(inCtx, func(pb restic.PackedBlob) {
		if pb.Type == restic.DataBlob {
			_, found := dstTestsRepo.LookupBlobSize(restic.DataBlob, pb.ID)
			if !found {
				stop(fmt.Errorf("blob %v expected but not found", pb.ID.Str()))
			}
		}
	})
	if err != nil {
		t.Error(err)
	}

	// 2) check if rechunk is done correctly by comparing rechunkMap
	testedRechunkMap := rechunker.rechunkMap
	for k, v := range wantedRechunkMap {
		wanted := HashOfIDs(v)
		tested := HashOfIDs(testedRechunkMap[k])
		if wanted != tested {
			t.Errorf("rechunk result for src file %v does not match: %v wanted, but got %v", k.Str(), wanted.Str(), tested.Str())
		}
	}
}

type BlobIDsPair struct {
	srcBlobIDs restic.IDs
	dstBlobIDs restic.IDs
}

func generateRandomBlobIDsPair(nSrc, nDst uint) BlobIDsPair {
	srcIDs := make(restic.IDs, 0, nSrc)
	dstIDs := make(restic.IDs, 0, nDst)
	for range nSrc {
		srcIDs = append(srcIDs, restic.NewRandomID())
	}
	for range nDst {
		dstIDs = append(dstIDs, restic.NewRandomID())
	}

	return BlobIDsPair{srcBlobIDs: srcIDs, dstBlobIDs: dstIDs}
}

// prepareTree prepares sample tree for rewriteTree test.
func prepareTree() (srcTree walker.TestTree, wantsTree walker.TestTree, rechunkMap map[restic.ID]restic.IDs) {
	blobIDsMap := map[string]BlobIDsPair{
		"a":        generateRandomBlobIDsPair(1, 1),
		"subdir/a": generateRandomBlobIDsPair(30, 31),
		"x":        generateRandomBlobIDsPair(42, 41),
		"0":        generateRandomBlobIDsPair(0, 0),
	}
	rechunkMap = map[restic.ID]restic.IDs{}
	for _, v := range blobIDsMap {
		rechunkMap[HashOfIDs(v.srcBlobIDs)] = v.dstBlobIDs
	}

	srcTree = walker.TestTree{
		"zerofile": walker.TestFile{
			Size:    0,
			Content: restic.IDs{},
		},
		"a": walker.TestFile{
			Size:    1,
			Content: blobIDsMap["a"].srcBlobIDs,
		},
		"x": walker.TestFile{
			Size:    2,
			Content: blobIDsMap["x"].srcBlobIDs,
		},
		"subdir": walker.TestTree{
			"a": walker.TestFile{
				Size:    3,
				Content: blobIDsMap["subdir/a"].srcBlobIDs,
			},
			"subdir": walker.TestTree{
				"dup_x": walker.TestFile{
					Size:    2,
					Content: blobIDsMap["x"].srcBlobIDs,
				},
			},
		},
	}
	wantsTree = walker.TestTree{
		"zerofile": walker.TestFile{
			Size:    0,
			Content: restic.IDs{},
		},
		"a": walker.TestFile{
			Size:    1,
			Content: blobIDsMap["a"].dstBlobIDs,
		},
		"x": walker.TestFile{
			Size:    2,
			Content: blobIDsMap["x"].dstBlobIDs,
		},
		"subdir": walker.TestTree{
			"a": walker.TestFile{
				Size:    3,
				Content: blobIDsMap["subdir/a"].dstBlobIDs,
			},
			"subdir": walker.TestTree{
				"dup_x": walker.TestFile{
					Size:    2,
					Content: blobIDsMap["x"].dstBlobIDs,
				},
			},
		},
	}

	return srcTree, wantsTree, rechunkMap
}

func TestRechunkerRewriteTree(t *testing.T) {
	srcTree, wantsTree, rechunkMap := prepareTree()

	srcRepo, srcRoot := walker.BuildTreeMap(srcTree)
	_, wantsRoot := walker.BuildTreeMap(wantsTree)

	testsRepo := data.TestWritableTreeMap{TestTreeMap: data.TestTreeMap{}}
	rechunker := NewRechunker(Config{})
	rechunker.rechunkMap = rechunkMap
	testsRoot, err := rechunker.RewriteTree(t.Context(), srcRepo, testsRepo, srcRoot)
	if err != nil {
		t.Error(err)
	}
	if wantsRoot != testsRoot {
		t.Errorf("tree mismatch. wants: %v, tests: %v", wantsRoot, testsRoot)
	}
}
