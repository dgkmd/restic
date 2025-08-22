package walker

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"sort"
	"testing"

	"github.com/restic/chunker"

	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"

	// borrowing test fixtures from following packages
	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/repository"
)

// Reference: walker_test.go, rewriter_test.go (v0.18.0)

func TestRechunkerRechunkData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	repository.TestUseLowSecurityKDFParameters(t)

	// prepare repositories
	srcRepo, _ := repository.New(repository.TestBackend(t), repository.Options{})
	dstTestsRepo, _ := repository.New(repository.TestBackend(t), repository.Options{})
	dstWantsRepo, _ := repository.New(repository.TestBackend(t), repository.Options{})

	srcChunkerParam, _ := chunker.RandomPolynomial()
	dstChunkerParam, _ := chunker.RandomPolynomial()

	repoVer := uint(restic.StableRepoVersion)
	repoPw := rtest.TestPassword
	_ = srcRepo.Init(ctx, repoVer, repoPw, &srcChunkerParam)
	_ = dstTestsRepo.Init(ctx, repoVer, repoPw, &dstChunkerParam)
	_ = dstWantsRepo.Init(ctx, repoVer, repoPw, &dstChunkerParam)

	// prepare test data
	src := archiver.TestDir{
		"0": archiver.TestFile{Content: ""},
		"1": archiver.TestFile{Content: string(rtest.Random(12, 10_000))},
		"2": archiver.TestFile{Content: string(rtest.Random(13, 20_000_000))},
	}
	dupFileContent := string(rtest.Random(16, 10_000_000))
	src["dup1"] = archiver.TestFile{Content: dupFileContent}
	src["dup2"] = archiver.TestFile{Content: dupFileContent}

	tempDir := rtest.TempDir(t)
	archiver.TestCreateFiles(t, tempDir, src)
	src = nil
	dupFileContent = ""

	// archive data to the repos with archiver
	srcSn := archiver.TestSnapshot(t, srcRepo, tempDir, nil)
	_ = archiver.TestSnapshot(t, dstWantsRepo, tempDir, nil)

	// do rechunking for dstTestRepo
	rechunker := NewRechunker(srcRepo, dstTestsRepo, dstTestsRepo.Config().ChunkerPolynomial)
	wg, wgCtx := errgroup.WithContext(ctx)
	dstTestsRepo.StartPackUploader(wgCtx, wg)
	rechunker.RechunkData(ctx, *srcSn.Tree, nil)
	dstTestsRepo.Flush(ctx)

	// compare data blobs between dstWantsRepo and dstTestRepo
	blobWants := restic.IDs{}
	dstWantsRepo.ListBlobs(ctx, func(pb restic.PackedBlob) {
		if pb.Type == restic.DataBlob {
			blobWants = append(blobWants, pb.ID)
		}
	})

	for _, blobID := range blobWants {
		_, ok := dstTestsRepo.LookupBlobSize(restic.DataBlob, blobID)
		if !ok {
			t.Errorf("Blob missing: %v", blobID)
		}
	}
}

type BlobIDsPair struct {
	srcBlobIDs restic.IDs
	dstBlobIDs restic.IDs
}

func generateBlobIDsPair(nSrc, nDst uint) BlobIDsPair {
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

type TestContentNode struct {
	Type    restic.NodeType
	Size    uint64
	Content restic.IDs
}

func BuildTreeMapExtended(tree TestTree) (m TreeMap, root restic.ID) {
	m = TreeMap{}
	id := buildTreeMapExtended(tree, m)
	return m, id
}

func buildTreeMapExtended(tree TestTree, m TreeMap) restic.ID {
	tb := restic.NewTreeJSONBuilder()
	var names []string
	for name := range tree {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		item := tree[name]
		switch elem := item.(type) {
		case TestFile:
			err := tb.AddNode(&restic.Node{
				Name: name,
				Type: restic.NodeTypeFile,
				Size: elem.Size,
			})
			if err != nil {
				panic(err)
			}
		case TestTree:
			id := buildTreeMapExtended(elem, m)
			err := tb.AddNode(&restic.Node{
				Name:    name,
				Subtree: &id,
				Type:    restic.NodeTypeDir,
			})
			if err != nil {
				panic(err)
			}
		case TestContentNode:
			err := tb.AddNode(&restic.Node{
				Name:    name,
				Type:    elem.Type,
				Size:    elem.Size,
				Content: elem.Content,
			})
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Sprintf("invalid type %T", elem))
		}
	}

	buf, err := tb.Finalize()
	if err != nil {
		panic(err)
	}

	id := restic.Hash(buf)

	if _, ok := m[id]; !ok {
		m[id] = buf
	}

	return id
}

func TestRechunkerRewriteTree(t *testing.T) {
	blobIDsMap := map[string]BlobIDsPair{
		"a":        generateBlobIDsPair(1, 1),
		"subdir/a": generateBlobIDsPair(30, 31),
		"x":        generateBlobIDsPair(42, 41),
		"0":        generateBlobIDsPair(0, 0),
	}
	rechunkBlobsMap := map[hashType]restic.IDs{}
	for _, v := range blobIDsMap {
		rechunkBlobsMap[hashOfIDs(v.srcBlobIDs)] = v.dstBlobIDs
	}

	tree := TestTree{
		"zerofile": TestContentNode{
			Type:    restic.NodeTypeFile,
			Size:    0,
			Content: restic.IDs{},
		},
		"a": TestContentNode{
			Type:    restic.NodeTypeFile,
			Size:    1,
			Content: blobIDsMap["a"].srcBlobIDs,
		},
		"subdir": TestTree{
			"a": TestContentNode{
				Type:    restic.NodeTypeFile,
				Size:    3,
				Content: blobIDsMap["subdir/a"].srcBlobIDs,
			},
			"x": TestContentNode{
				Type:    restic.NodeTypeFile,
				Size:    2,
				Content: blobIDsMap["x"].srcBlobIDs,
			},
			"subdir": TestTree{
				"dup_x": TestContentNode{
					Type:    restic.NodeTypeFile,
					Size:    2,
					Content: blobIDsMap["x"].srcBlobIDs,
				},
				"nonregularfile": TestContentNode{
					Type: restic.NodeTypeSymlink,
				},
			},
		},
	}
	wants := TestTree{
		"zerofile": TestContentNode{
			Type:    restic.NodeTypeFile,
			Size:    0,
			Content: restic.IDs{},
		},
		"a": TestContentNode{
			Type:    restic.NodeTypeFile,
			Size:    1,
			Content: blobIDsMap["a"].dstBlobIDs,
		},
		"subdir": TestTree{
			"a": TestContentNode{
				Type:    restic.NodeTypeFile,
				Size:    3,
				Content: blobIDsMap["subdir/a"].dstBlobIDs,
			},
			"x": TestContentNode{
				Type:    restic.NodeTypeFile,
				Size:    2,
				Content: blobIDsMap["x"].dstBlobIDs,
			},
			"subdir": TestTree{
				"dup_x": TestContentNode{
					Type:    restic.NodeTypeFile,
					Size:    2,
					Content: blobIDsMap["x"].dstBlobIDs,
				},
				"nonregularfile": TestContentNode{
					Type: restic.NodeTypeSymlink,
				},
			},
		},
	}

	srcRepo, srcRoot := BuildTreeMapExtended(tree)
	_, wantsRoot := BuildTreeMapExtended(wants)

	testsRepo := WritableTreeMap{TreeMap{}}
	rechunker := NewRechunker(srcRepo, testsRepo, 0)
	rechunker.rechunkBlobsMap = rechunkBlobsMap
	testsRoot, err := rechunker.RewriteTree(context.TODO(), srcRoot)
	if err != nil {
		t.Error(err)
	}
	if wantsRoot != testsRoot {
		t.Errorf("tree mismatch. wants: %v, tests: %v", wantsRoot, testsRoot)
	}
}
