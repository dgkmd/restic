package rechunker

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
	hashval hashType
}
type blobLookupInfo struct {
	dataLength uint
	packID     restic.ID
}

type srcChunk struct {
	seqNum int
	blob   []byte
}
type newPipeSpec struct {
	seqNum int
	reader *io.PipeReader
}
type dstChunk struct {
	seqNum int
	chunk  chunker.Chunk
}
type jumpSpec struct {
	seqNum  int
	blobIdx int
	offset  uint
}

type PackBlobLoader interface {
	LoadBlobsFromPack(ctx context.Context, packID restic.ID, blobs []restic.Blob, handleBlobFn func(blob restic.BlobHandle, buf []byte, err error) error) error
}

var ErrNewSequence = errors.New("new sequence")

type Rechunker struct {
	pol       chunker.Pol
	chainDict *RechunkChainDict

	// read-only once built
	filesList    []restic.IDs
	blobLookup   map[restic.ID]blobLookupInfo // blob ID -> {blob length, pack ID}
	sPackToFiles map[restic.ID][]fileType     // pack ID -> list of files{srcBlobIDs, hashOfIDs} that contains any blob in the pack
	rechunkReady bool

	// concurrently accessed across workers
	rechunkMap        map[hashType]restic.IDs     // hashOfIDs of srcBlobIDs -> dstBlobIDs
	packToBlobs       map[restic.ID][]restic.Blob // pack ID -> list of blobs to be loaded from the pack
	sPackRequires     map[hashType]int            // hashOfIDs of srcBlobIDs -> number of packs until all blobs ready in the cache
	rechunkMapLock    sync.Mutex
	packToBlobsLock   sync.RWMutex
	sPackRequiresLock sync.Mutex

	// used in RewriteTree
	rewriteTreeMap map[restic.ID]restic.ID // original tree ID (in src repo) -> rewritten tree ID (for dst repo)
}

func NewRechunker(pol chunker.Pol) *Rechunker {
	return &Rechunker{
		pol:            pol,
		chainDict:      NewRechunkChainDict(),
		rechunkMap:     map[hashType]restic.IDs{},
		rewriteTreeMap: map[restic.ID]restic.ID{},
	}
}

const SMALL_FILE_THRESHOLD = 25
const LARGE_FILE_THRESHOLD = 25

func (rc *Rechunker) reset() {
	rc.filesList = nil
	rc.blobLookup = map[restic.ID]blobLookupInfo{}
	rc.sPackToFiles = map[restic.ID][]fileType{}
	rc.rechunkReady = false

	rc.sPackRequires = map[hashType]int{}

	rc.packToBlobs = map[restic.ID][]restic.Blob{}
}

func (rc *Rechunker) buildIndex(lookupBlobFn func(t restic.BlobType, id restic.ID) []restic.PackedBlob) error {
	// collect all required blobs
	allBlobs := restic.IDSet{}
	for _, file := range rc.filesList {
		for _, blob := range file {
			allBlobs.Insert(blob)
		}
	}

	// build pack<->blob map
	for blob := range allBlobs {
		packs := lookupBlobFn(restic.DataBlob, blob)
		if len(packs) == 0 {
			return fmt.Errorf("can't find blob from source repo: %v", blob)
		}
		pb := packs[0]

		rc.packToBlobs[pb.PackID] = append(rc.packToBlobs[pb.PackID], pb.Blob)
		rc.blobLookup[pb.Blob.ID] = blobLookupInfo{
			dataLength: pb.DataLength(),
			packID:     pb.PackID,
		}
	}

	// build pack<->file metadata for small files
	for _, file := range rc.filesList {
		if len(file) > SMALL_FILE_THRESHOLD {
			continue
		}
		hashval := hashOfIDs(file)
		packSet := restic.IDSet{}
		for _, blob := range file {
			pack := rc.blobLookup[blob].packID
			packSet.Insert(pack)
		}
		rc.sPackRequires[hashval] = len(packSet)
		for p := range packSet {
			rc.sPackToFiles[p] = append(rc.sPackToFiles[p], fileType{file, hashval})
		}
	}

	return nil
}

func (rc *Rechunker) Plan(ctx context.Context, srcRepo restic.Repository, rootTrees []restic.ID) error {
	rc.reset()

	visitedFiles := map[hashType]struct{}{}
	visitedTrees := restic.IDSet{}

	// skip previously processed files and trees
	for k := range rc.rechunkMap {
		visitedFiles[k] = struct{}{}
	}
	for k := range rc.rewriteTreeMap {
		visitedTrees.Insert(k)
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	treeStream := restic.StreamTrees(wgCtx, wg, srcRepo, rootTrees, func(id restic.ID) bool {
		visited := visitedTrees.Has(id)
		visitedTrees.Insert(id)
		return visited
	}, nil)

	// gather all distinct file Contents under trees
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

	err = rc.buildIndex(srcRepo.LookupBlob)
	if err != nil {
		return err
	}

	rc.rechunkReady = true

	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, srcRepo PackBlobLoader, dstRepo restic.BlobSaver, p *progress.Counter) error {
	if !rc.rechunkReady {
		return fmt.Errorf("Plan() must be run first before RechunkData()")
	}
	rc.rechunkReady = false

	wgMgr, wgMgrCtx := errgroup.WithContext(ctx)
	wgWkr, wgWkrCtx := errgroup.WithContext(ctx)

	numWorkers := max(runtime.GOMAXPROCS(0)-1, 1)
	numDownloaders := max(numWorkers/4, 1)
	var priorityFilesList []restic.IDs
	var priorityFilesListLock sync.Mutex

	// blob cache
	cache := NewRechunkBlobCache(wgMgrCtx, wgMgr, rc.blobLookup, numDownloaders, func(packID restic.ID) (BlobData, error) {
		// downloadFn implementation
		blobData := BlobData{}
		rc.packToBlobsLock.RLock()
		blobs := rc.packToBlobs[packID]
		rc.packToBlobsLock.RUnlock()
		err := srcRepo.LoadBlobsFromPack(wgMgrCtx, packID, blobs,
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
		filesToUpdate := rc.sPackToFiles[packID]
		var readyFiles []restic.IDs

		rc.sPackRequiresLock.Lock()
		for _, file := range filesToUpdate {
			if rc.sPackRequires[file.hashval] > 0 {
				rc.sPackRequires[file.hashval]--
				if rc.sPackRequires[file.hashval] == 0 {
					readyFiles = append(readyFiles, file.IDs)
				}
			}
		}
		rc.sPackRequiresLock.Unlock()

		priorityFilesListLock.Lock()
		priorityFilesList = append(priorityFilesList, readyFiles...)
		priorityFilesListLock.Unlock()
	}, func(packID restic.ID) {
		// onPackEvict implementation
		filesToUpdate := rc.sPackToFiles[packID]
		rc.sPackRequiresLock.Lock()
		for _, file := range filesToUpdate {
			// files with sPackRequires==0 has already gone to priorityFilesList, so don't track them
			if rc.sPackRequires[file.hashval] > 0 {
				rc.sPackRequires[file.hashval]++
			}
		}
		rc.sPackRequiresLock.Unlock()
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
		regularTrack := rc.filesList
		var fastTrack []restic.IDs

		for {
			if len(fastTrack) == 0 {
				priorityFilesListLock.Lock()
				if len(priorityFilesList) > 0 {
					fastTrack = priorityFilesList
					priorityFilesList = nil
				}
				priorityFilesListLock.Unlock()
			}

			if len(fastTrack) > 0 {
				file := fastTrack[0]
				fastTrack = fastTrack[1:]
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
			} else if len(regularTrack) > 0 {
				file := regularTrack[0]
				regularTrack = regularTrack[1:]
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

				chSrcChunk := make(chan srcChunk)
				chNewPipe := make(chan newPipeSpec) // used only if useChainDict
				chDstChunk := make(chan dstChunk)
				chJumpOrder := make(chan jumpSpec, 1) // used only if useChainDict
				r, w := io.Pipe()
				chnker.Reset(r, rc.pol)
				wg, wgCtx := errgroup.WithContext(wgWkrCtx)

				// prepare variables for chainDict
				var blobPos []uint
				var seekBlobPos func(uint, int) (int, uint)
				var prefixPos uint
				var prefixIdx int
				useChainDict := len(srcBlobs) > LARGE_FILE_THRESHOLD
				if useChainDict {
					// build blobPos
					blobPos = make([]uint, len(srcBlobs)+1)
					var offset uint
					for i, blob := range srcBlobs {
						offset += rc.blobLookup[blob].dataLength
						blobPos[i+1] = offset
					}

					// define seekBlobPos
					seekBlobPos = func(pos uint, seekStartIdx int) (int, uint) {
						if pos < blobPos[seekStartIdx] { // invalid pos
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
					prefixBlobs, numFinishedBlobs, newOffset := rc.chainDict.Match(srcBlobs, 0)
					if len(prefixBlobs) > 0 {
						prefixIdx = numFinishedBlobs
						prefixPos = blobPos[numFinishedBlobs] + newOffset
						dstBlobs = prefixBlobs

						chJumpOrder <- jumpSpec{
							seqNum:  0,
							blobIdx: numFinishedBlobs,
							offset:  newOffset,
						}
					}
				}

				// run loader/iopipe/chunker/saver Goroutines per each file
				// loader: load original chunks one by one from source repo
				wg.Go(func() error {
					var seqNum int  // used only if useChainDict
					var offset uint // used only if useChainDict

				MainLoop:
					for i := 0; i < len(srcBlobs); i++ {
						if useChainDict {
							select {
							case newPos := <-chJumpOrder:
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
						if useChainDict && offset != 0 {
							copy(blob, blob[offset:])
							blob = blob[:len(blob)-int(offset)]
							offset = 0
						}

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chSrcChunk <- srcChunk{seqNum, blob}:
						}
					}
					close(chSrcChunk)
					return nil
				})

				// iopipe: convert chunks into io.Reader stream
				wg.Go(func() error {
					var seqNum int // used only if useChainDict

					for {
						var c srcChunk
						var ok bool
						select {
						case <-wgCtx.Done():
							w.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case c, ok = <-chSrcChunk:
							if !ok { // EOF
								err := w.Close()
								return err
							}
						}

						if useChainDict && c.seqNum > seqNum {
							// new sequence
							seqNum = c.seqNum
							w.CloseWithError(ErrNewSequence)
							r, w = io.Pipe()
							select {
							case <-wgCtx.Done():
								return wgCtx.Err()
							case chNewPipe <- newPipeSpec{seqNum, r}:
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
					var seqNum int // used only if useChainDict

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
							close(chDstChunk)
							return nil
						}
						if useChainDict && err == ErrNewSequence {
							select {
							case bufferPool <- buf:
							default:
							}
							select {
							case <-wgCtx.Done():
								return wgCtx.Err()
							case newPipe := <-chNewPipe:
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
						case chDstChunk <- dstChunk{seqNum, chunk}:
						}
					}
				})

				// saver: save rechunked blobs into destination repo
				wg.Go(func() error {
					var seqNum int       // used only if useChainDict
					currIdx := prefixIdx // used only if useChainDict
					currPos := prefixPos // used only if useChainDict

					for {
						var c dstChunk
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case c, ok = <-chDstChunk:
							if !ok { // EOF
								return nil
							}
						}

						if useChainDict && c.seqNum < seqNum {
							// this chunk is skipped by previous chainDict match, so just throw it away and continue to next chunk
							select {
							case bufferPool <- c.chunk.Data:
							default:
							}
							continue
						}

						blobData := c.chunk.Data
						dstBlobID, _, _, err := dstRepo.SaveBlob(ctx, restic.DataBlob, blobData, restic.ID{}, false)
						if err != nil {
							return err
						}

						if useChainDict { // add chunk to chainDict
							startOffset := currPos - blobPos[currIdx]
							endPos := currPos + c.chunk.Length
							endIdx, endOffset := seekBlobPos(endPos, currIdx)

							var chunkSrcBlobs []restic.ID
							if endIdx == len(srcBlobs) {
								chunkSrcBlobs = make([]restic.ID, endIdx-currIdx+1)
								n := copy(chunkSrcBlobs, srcBlobs[currIdx:endIdx]) // last element of chunkSrcBlobs is nullID
								if n != endIdx-currIdx {
									return fmt.Errorf("srcBlobs tail copy failed")
								}
							} else {
								chunkSrcBlobs = srcBlobs[currIdx : endIdx+1]
							}

							err := rc.chainDict.Store(chunkSrcBlobs, startOffset, endOffset, dstBlobID)
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

						if useChainDict { // match chunks from chainDict and append them
							currOffset := currPos - blobPos[currIdx]
							matchedDstBlobs, numFinishedSrcBlobs, newOffset := rc.chainDict.Match(srcBlobs[currIdx:], currOffset)
							if numFinishedSrcBlobs > 4 { // apply only when you can skip many blobs; otherwise, it would be better not to interrupt the pipeline
								dstBlobs = append(dstBlobs, matchedDstBlobs...)

								currIdx += numFinishedSrcBlobs
								currPos = blobPos[currIdx] + newOffset

								seqNum++
								chJumpOrder <- jumpSpec{
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

func (rc *Rechunker) RewriteTree(ctx context.Context, srcRepo restic.BlobLoader, dstRepo restic.BlobSaver, nodeID restic.ID) (restic.ID, error) {
	// check if the identical tree has already been processed
	newID, ok := rc.rewriteTreeMap[nodeID]
	if ok {
		return newID, nil
	}

	curTree, err := restic.LoadTree(ctx, srcRepo, nodeID)
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
		newID, err := rc.RewriteTree(ctx, srcRepo, dstRepo, subtree)
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
	newTreeID, _, _, err := dstRepo.SaveBlob(ctx, restic.TreeBlob, tree, restic.ID{}, false)
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
