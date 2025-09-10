package rechunker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/restic/chunker"

	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// Reference: walker_test.go, rewriter_test.go (v0.18.0)

type TestRechunkerRepo struct {
	loadBlobsFromPack func(packID restic.ID, blobs []restic.Blob, handleBlobFn func(blob restic.BlobHandle, buf []byte, err error) error) error
	saveBlob          func(buf []byte) (newID restic.ID, known bool, size int, err error)
}

func (trr *TestRechunkerRepo) LoadBlobsFromPack(ctx context.Context, packID restic.ID, blobs []restic.Blob, handleBlobFn func(blob restic.BlobHandle, buf []byte, err error) error) error {
	return trr.loadBlobsFromPack(packID, blobs, handleBlobFn)
}
func (trr *TestRechunkerRepo) SaveBlob(ctx context.Context, t restic.BlobType, buf []byte, id restic.ID, storeDuplicate bool) (newID restic.ID, known bool, size int, err error) {
	return trr.saveBlob(buf)
}

func chunkFiles(chnker *chunker.Chunker, pol chunker.Pol, files map[string][]byte) (map[string]restic.IDs, map[restic.ID][]byte) {
	chunkedFiles := map[string]restic.IDs{}
	blobStore := map[restic.ID][]byte{}

	for name, data := range files {
		r := bytes.NewReader(data)
		chnker.Reset(r, pol)
		chunks := restic.IDs{}

		for {
			chunk, err := chnker.Next(nil)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			id := restic.Hash(chunk.Data)
			chunks = append(chunks, id)
			if _, ok := blobStore[id]; !ok {
				blobStore[id] = chunk.Data
			}
		}

		chunkedFiles[name] = chunks
	}

	return chunkedFiles, blobStore
}

func simulatedPack(blobStore map[restic.ID][]byte) map[restic.ID]restic.ID {
	blobToPack := map[restic.ID]restic.ID{}
	i := 0
	packID := restic.NewRandomID()
	for blobID := range blobStore {
		blobToPack[blobID] = packID
		i++
		if i%10 == 0 {
			packID = restic.NewRandomID()
		}
	}

	return blobToPack
}

func TestRechunkerRechunkData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	srcChunkerParam, _ := chunker.RandomPolynomial()
	dstChunkerParam, _ := chunker.RandomPolynomial()

	// prepare test data
	files := map[string][]byte{
		"0": {},
		"1": rtest.Random(1, 10_000),
		"2": rtest.Random(2, 200_000),
		"3": rtest.Random(3, 10_000_000),
		"4": rtest.Random(4, 20_000_000),
		"5": rtest.Random(5, 50_000_000),
		"6": rtest.Random(6, 100_000_000),
	}
	files["3_duplicate"] = files["3"]
	files["6_smiliar"] = append(rtest.Random(7, 10000), files["6"][10000:]...)

	chnker := chunker.New(nil, 0)
	srcChunkedFiles, srcBlobStore := chunkFiles(chnker, srcChunkerParam, files)
	dstWantsChunkedFiles, _ := chunkFiles(chnker, dstChunkerParam, files)
	dstTestsBlobStore := restic.IDSet{}

	srcFilesList := []restic.IDs{}
	for _, file := range srcChunkedFiles {
		srcFilesList = append(srcFilesList, file)
	}
	srcBlobToPack := simulatedPack(srcBlobStore)

	// do rechunking for dstTestRepo
	rechunker := NewRechunker(dstChunkerParam)
	rechunker.reset()
	rechunker.filesList = srcFilesList
	rechunker.buildIndex(func(t restic.BlobType, id restic.ID) []restic.PackedBlob {
		pb := restic.PackedBlob{}
		pb.ID = id
		pb.Type = t
		pb.UncompressedLength = uint(len(srcBlobStore[id]))
		pb.PackID = srcBlobToPack[id]

		return []restic.PackedBlob{pb}
	})
	rechunker.rechunkReady = true

	srcRepo := &TestRechunkerRepo{
		loadBlobsFromPack: func(packID restic.ID, blobs []restic.Blob, handleBlobFn func(blob restic.BlobHandle, buf []byte, err error) error) error {
			for _, blob := range blobs {
				if packID != srcBlobToPack[blob.ID] {
					return fmt.Errorf("blob %v is not in the pack %v", blob.ID, packID)
				}
				handleBlobFn(blob.BlobHandle, srcBlobStore[blob.ID], nil)
			}
			return nil
		},
	}
	dstTestsRepo := &TestRechunkerRepo{
		saveBlob: func(buf []byte) (newID restic.ID, known bool, size int, err error) {
			newID = restic.Hash(buf)
			dstTestsBlobStore.Insert(newID)
			return
		},
	}
	if err := rechunker.RechunkData(ctx, srcRepo, dstTestsRepo, nil); err != nil {
		panic(err)
	}

	// compare data blobs between dstWantsRepo and dstTestRepo
	rechunkMap := rechunker.rechunkMap
	for name, srcBlobs := range srcChunkedFiles {
		hashval := hashOfIDs(srcBlobs)
		if hashOfIDs(dstWantsChunkedFiles[name]) != hashOfIDs(rechunkMap[hashval]) {
			t.Errorf("rechunk not correct for %v", name)
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

type TreeMap map[restic.ID][]byte
type TestTree map[string]interface{}
type TestContentNode struct {
	Type    restic.NodeType
	Size    uint64
	Content restic.IDs
}

func (t TreeMap) LoadBlob(_ context.Context, _ restic.BlobType, id restic.ID, _ []byte) ([]byte, error) {
	buf, ok := t[id]
	if !ok {
		return nil, fmt.Errorf("blob does not exist")
	}
	return buf, nil
}

func (t TreeMap) SaveBlob(_ context.Context, _ restic.BlobType, buf []byte, _ restic.ID, _ bool) (newID restic.ID, known bool, size int, err error) {
	id := restic.Hash(buf)

	_, ok := t[id]
	if ok {
		return id, false, 0, nil
	}

	t[id] = append([]byte{}, buf...)
	return id, true, len(buf), nil
}

func BuildTreeMap(tree TestTree) (m TreeMap, root restic.ID) {
	m = TreeMap{}
	id := buildTreeMap(tree, m)
	return m, id
}

func buildTreeMap(tree TestTree, m TreeMap) restic.ID {
	tb := restic.NewTreeJSONBuilder()
	var names []string
	for name := range tree {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		item := tree[name]
		switch elem := item.(type) {
		case TestTree:
			id := buildTreeMap(elem, m)
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

	srcRepo, srcRoot := BuildTreeMap(tree)
	_, wantsRoot := BuildTreeMap(wants)

	testsRepo := TreeMap{}
	rechunker := NewRechunker(0)
	rechunker.reset()
	rechunker.rechunkMap = rechunkBlobsMap
	testsRoot, err := rechunker.RewriteTree(context.TODO(), srcRepo, testsRepo, srcRoot)
	if err != nil {
		t.Error(err)
	}
	if wantsRoot != testsRoot {
		t.Errorf("tree mismatch. wants: %v, tests: %v", wantsRoot, testsRoot)
	}
}
