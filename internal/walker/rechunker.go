package walker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
	"golang.org/x/sync/errgroup"
)

type hashType = [32]byte
type fileType struct {
	restic.IDs
	blobsHash hashType
}
type blobInfo struct {
	length uint
	packID restic.ID
}

type chFrontType struct {
	seqNum int
	blob   []byte
}
type chMidType struct {
	seqNum int
	reader *io.PipeReader
}
type chBackType struct {
	seqNum int
	chunk  chunker.Chunk
}
type chJumpType struct {
	seqNum  int
	blobIdx int
	offset  uint
}

var ErrNewSequence = errors.New("new sequence")

type Rechunker struct {
	srcRepo    restic.Repository
	dstRepo    restic.Repository
	pol        chunker.Pol
	blobLoader restic.BlobLoader
	blobSaver  restic.BlobSaver
	chainDict  *RechunkChainDict

	// read-only once built
	filesList   []restic.IDs
	blobLookup  map[restic.ID]blobInfo
	packToBlobs map[restic.ID][]restic.Blob

	// RW by dedicated worker
	blobRequires  map[restic.ID]int
	sPackRequires map[hashType]int
	sPackToFiles  map[restic.ID][]fileType

	// concurrently accessed across workers
	priorityFilesList     []restic.IDs
	rechunkMap            map[hashType]restic.IDs
	priorityFilesListLock sync.Mutex
	rechunkMapLock        sync.Mutex

	// used in RewriteTree
	rewriteTreeMap map[restic.ID]restic.ID
}

func NewRechunker(srcRepo restic.Repository, dstRepo restic.Repository) *Rechunker {
	return &Rechunker{
		srcRepo:        srcRepo,
		dstRepo:        dstRepo,
		pol:            dstRepo.Config().ChunkerPolynomial,
		blobLoader:     srcRepo,
		blobSaver:      dstRepo,
		chainDict:      NewRechunkChainDict(),
		rechunkMap:     map[hashType]restic.IDs{},
		rewriteTreeMap: map[restic.ID]restic.ID{},
	}
}

const SMALL_FILE_THRESHOLD = 25
const LARGE_FILE_THRESHOLD = 50

func (rc *Rechunker) Reset() {
	rc.filesList = nil
	rc.priorityFilesList = nil

	rc.blobLookup = map[restic.ID]blobInfo{}
	rc.packToBlobs = map[restic.ID][]restic.Blob{}
	rc.blobRequires = map[restic.ID]int{}
	rc.sPackRequires = map[hashType]int{}
	rc.sPackToFiles = map[restic.ID][]fileType{}
}

func (rc *Rechunker) Plan(ctx context.Context, roots []restic.ID) error {
	rc.Reset()

	visitedFiles := map[hashType]struct{}{}
	visitedTrees := restic.IDSet{}

	// skip previously processed files and trees
	for k := range rc.rechunkMap {
		visitedFiles[k] = struct{}{}
	}
	for k := range rc.rewriteTreeMap {
		visitedTrees[k] = struct{}{}
	}

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

					rc.filesList = append(rc.filesList, node.Content)
				}
			}
		}
		return nil
	})
	err := wg.Wait()
	if err != nil {
		return err
	}

	// count how many times each blob is needed
	for _, blobs := range rc.filesList {
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

		rc.packToBlobs[pb.PackID] = append(rc.packToBlobs[pb.PackID], pb.Blob)
		rc.blobLookup[pb.Blob.ID] = blobInfo{
			length: pb.DataLength(),
			packID: pb.PackID,
		}
	}

	// build pack<->file metadata for small files
	for _, file := range rc.filesList {
		if len(file) >= SMALL_FILE_THRESHOLD {
			continue
		}
		hashval := hashOfIDs(file)
		packSet := restic.IDSet{}
		for _, blob := range file {
			pack := rc.blobLookup[blob].packID
			packSet[pack] = struct{}{}
		}
		rc.sPackRequires[hashval] = len(packSet)
		for p := range packSet {
			rc.sPackToFiles[p] = append(rc.sPackToFiles[p], fileType{file, hashval})
		}
	}

	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, p *progress.Counter) error {
	wgMgr, wgMgrCtx := errgroup.WithContext(ctx)
	wgWkr, wgWkrCtx := errgroup.WithContext(ctx)

	// blob cache
	cache := NewRechunkBlobCache(wgMgrCtx, wgMgr, rc.blobLookup, func(packID restic.ID) (BlobData, error) {
		// downloadFn implementation
		blobData := BlobData{}
		blobs := rc.packToBlobs[packID]
		err := rc.srcRepo.LoadBlobsFromPack(wgMgrCtx, packID, blobs,
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
	}, func(packID restic.ID) {
		// onPackReady implementation
		files := rc.sPackToFiles[packID]
		for _, file := range files {
			if rc.sPackRequires[file.blobsHash] > 0 {
				rc.sPackRequires[file.blobsHash]--
				if rc.sPackRequires[file.blobsHash] == 0 {
					rc.priorityFilesListLock.Lock()
					rc.priorityFilesList = append(rc.priorityFilesList, file.IDs)
					rc.priorityFilesListLock.Unlock()
				}
			}
		}
	}, func(packID restic.ID) {
		// onPackEvict implementation
		files := rc.sPackToFiles[packID]
		file_remaining := false
		for _, file := range files {
			// files with sPackRequires==0 has already gone to priorityFilesList, so ignore them
			if rc.sPackRequires[file.blobsHash] > 0 {
				rc.sPackRequires[file.blobsHash]++
				file_remaining = true
			}
			if !file_remaining {
				rc.sPackToFiles[packID] = []fileType{}
			}
		}
	})

	// implement blobGet using blob cache
	blobGet := func(blobID restic.ID, buf []byte) ([]byte, error) {
		blob, ch := cache.Get(wgWkrCtx, wgWkr, blobID, buf)
		if blob == nil { // wait for blob to be downloaded
			select {
			case <-wgWkrCtx.Done():
				return nil, wgWkrCtx.Err()
			case blob = <-ch:
			}
		}
		return blob, nil
	}

	// job dispatcher
	chDispatch := make(chan restic.IDs)
	wgMgr.Go(func() error {
		seenFiles := map[hashType]struct{}{}
		ordinaryList := rc.filesList
		var priorityList []restic.IDs

		for {
			if len(priorityList) == 0 {
				rc.priorityFilesListLock.Lock()
				if len(rc.priorityFilesList) > 0 {
					priorityList = rc.priorityFilesList
					rc.priorityFilesList = nil
				}
				rc.priorityFilesListLock.Unlock()
			}

			if len(priorityList) > 0 {
				file := priorityList[0]
				priorityList = priorityList[1:]
				hashval := hashOfIDs(file)
				if _, ok := seenFiles[hashval]; ok {
					continue
				}
				seenFiles[hashval] = struct{}{}

				select {
				case <-wgMgrCtx.Done():
					return wgMgrCtx.Err()
				case chDispatch <- file:
				}
			} else if len(ordinaryList) > 0 {
				file := ordinaryList[0]
				ordinaryList = ordinaryList[1:]
				hashval := hashOfIDs(file)
				if _, ok := seenFiles[hashval]; ok {
					continue
				}
				seenFiles[hashval] = struct{}{}

				select {
				case <-wgMgrCtx.Done():
					return wgMgrCtx.Err()
				case chDispatch <- file:
				}
			} else { // no more jobs
				close(chDispatch)
				return nil
			}
		}
	})

	// rechunk workers
	numWorkers := max(runtime.GOMAXPROCS(0)-1, 1)
	bufferPool := make(chan []byte, 4*numWorkers)
	for range numWorkers {
		wgWkr.Go(func() error {
			chnker := chunker.New(nil, rc.pol)

			for {
				var srcBlobs restic.IDs
				var ok bool
				select {
				case <-wgWkrCtx.Done():
					return wgWkrCtx.Err()
				case srcBlobs, ok = <-chDispatch:
					if !ok { // all files finished and chan closed
						return nil
					}
				}
				dstBlobs := restic.IDs{}

				chFront := make(chan chFrontType)
				chMid := make(chan chMidType) // used only for large files
				chBack := make(chan chBackType)
				chJump := make(chan chJumpType, 1) // used only for large files
				r, w := io.Pipe()
				chnker.Reset(r, rc.pol)
				wg, wgCtx := errgroup.WithContext(wgWkrCtx)

				// preparation for large files
				var blobPos []uint
				var seekBlobPos func(uint, int) (int, uint)
				var prefixPos uint
				var prefixIdx int
				isLargeFile := len(srcBlobs) > LARGE_FILE_THRESHOLD
				if isLargeFile {
					// build blobPos
					blobPos = make([]uint, len(srcBlobs)+1)
					var offset uint
					for i, blob := range srcBlobs {
						offset += rc.blobLookup[blob].length
						blobPos[i+1] = offset
					}

					// define seekBlobPos
					seekBlobPos = func(pos uint, seekStartIdx int) (int, uint) {
						if pos < blobPos[seekStartIdx] { // invalid position
							return -1, 0
						}
						i := seekStartIdx
						for i < len(srcBlobs) && pos >= blobPos[i+1] {
							i++
						}
						offset := pos - blobPos[i]

						return i, offset
					}

					// prefix match
					prefixBlobs, numFinishedBlobs, newOffset := rc.chainDict.Get(srcBlobs, 0)
					if prefixBlobs != nil {
						prefixIdx = numFinishedBlobs
						prefixPos = blobPos[numFinishedBlobs] + newOffset
						dstBlobs = prefixBlobs
						chJump <- chJumpType{
							seqNum:  0,
							blobIdx: numFinishedBlobs,
							offset:  newOffset,
						}
					}
				}

				// run loader/iopipe/chunker/saver Goroutines per each file
				// loader: load original chunks one by one from source repo
				wg.Go(func() error {
					var seqNum int
					var offset uint

				MainLoop:
					for i := 0; i < len(srcBlobs); i++ {
						if isLargeFile {
							select {
							case newPos := <-chJump:
								seqNum = newPos.seqNum
								i = newPos.blobIdx
								offset = newPos.offset
								if i >= len(srcBlobs) {
									break MainLoop
								}
							default:
							}
						}

						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							buf = make([]byte, chunker.MaxSize)
						}

						blob, err := blobGet(srcBlobs[i], buf)
						if err != nil {
							return err
						}
						if offset != 0 {
							copy(blob, blob[offset:])
							blob = blob[:len(blob)-int(offset)]
							offset = 0
						}

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chFront <- chFrontType{seqNum, blob}:
						}
					}
					close(chFront)
					return nil
				})

				// iopipe: convert chunks into io.Reader stream
				wg.Go(func() error {
					var seqNum int

					for {
						var c chFrontType
						var ok bool
						select {
						case <-wgCtx.Done():
							w.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case c, ok = <-chFront:
							if !ok { // EOF
								w.Close()
								return nil
							}
						}

						if isLargeFile && c.seqNum > seqNum {
							// new sequence
							seqNum = c.seqNum
							w.CloseWithError(ErrNewSequence)
							r, w = io.Pipe()
							select {
							case <-ctx.Done():
								return ctx.Err()
							case chMid <- chMidType{seqNum, r}:
							}
						}

						buf := c.blob
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
				wg.Go(func() error {
					var seqNum int

					for {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
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
						if isLargeFile && err == ErrNewSequence {
							select {
							case bufferPool <- buf:
							default:
							}
							select {
							case <-ctx.Done():
								return ctx.Err()
							case newPipe := <-chMid:
								seqNum = newPipe.seqNum
								r = newPipe.reader
								chnker.Reset(r, rc.pol)
							}
							continue
						}
						if err != nil {
							r.CloseWithError(err)
							return err
						}

						select {
						case <-wgCtx.Done():
							r.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case chBack <- chBackType{seqNum, chunk}:
						}
					}
				})

				// saver: save rechunked blobs into destination repo
				wg.Go(func() error {
					var seqNum int
					var currIdx int = prefixIdx
					var currPos uint = prefixPos

					for {
						var c chBackType
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case c, ok = <-chBack:
							if !ok { // EOF
								return nil
							}
						}

						if isLargeFile && c.seqNum < seqNum {
							select {
							case bufferPool <- c.chunk.Data:
							default:
							}
							continue
						}

						blobData := c.chunk.Data
						dstBlobID, _, _, err := rc.dstRepo.SaveBlob(ctx, restic.DataBlob, blobData, restic.ID{}, false)
						if err != nil {
							return err
						}

						if isLargeFile { // add chunk to chainDict
							startOffset := currPos - blobPos[currIdx]
							endPos := currPos + c.chunk.Length
							endIdx, endOffset := seekBlobPos(endPos, currIdx)

							var chunkSrcBlobs []restic.ID
							if endIdx == len(srcBlobs) {
								chunkSrcBlobs = append(srcBlobs[currIdx:endIdx], rc.chainDict.nullID)
							} else {
								chunkSrcBlobs = srcBlobs[currIdx : endIdx+1]
							}

							err := rc.chainDict.Add(chunkSrcBlobs, startOffset, endOffset, dstBlobID)
							if err != nil {
								return err
							}

							currPos = endPos
							currIdx = endIdx
						}

						select {
						case bufferPool <- blobData:
						default:
						}
						dstBlobs = append(dstBlobs, dstBlobID)

						if isLargeFile { // append chunks from chainDict
							currOffset := currPos - blobPos[currIdx]
							appendBlobs, numFinishedBlobs, newOffset := rc.chainDict.Get(srcBlobs[currIdx:], currOffset)
							if numFinishedBlobs > 5 { // jump only when you can jump far
								dstBlobs = append(dstBlobs, appendBlobs...)
								currIdx += numFinishedBlobs
								currPos = blobPos[currIdx] + newOffset
								seqNum++
								chJump <- chJumpType{
									seqNum:  seqNum,
									blobIdx: currIdx,
									offset:  newOffset,
								}
							}
						}
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
	// shutdown management workers
	cache.Close()
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

func (rc *Rechunker) NumFilesToProcess() int {
	return len(rc.filesList)
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
