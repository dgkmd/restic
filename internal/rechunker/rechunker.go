package rechunker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
	"golang.org/x/sync/errgroup"
)

// data structure for debug trace
var debugNote = map[string]int{}
var debugNoteLock = sync.Mutex{}

type chunkedFile struct {
	restic.IDs
	hashval restic.ID
}
type fileStreamReader struct {
	*io.PipeReader
	stNum int
}
type chunk struct {
	chunker.Chunk
	stNum int
}
type fastForward struct {
	newStNum int
	blobIdx  int
	offset   uint
}

var ErrNewStream = errors.New("new stream")

type getBlobFn func(blobID restic.ID, buf []byte) ([]byte, error)
type seekBlobPosFn func(pos uint, seekStartIdx int) (idx int, offset uint)
type dictStoreFn func(srcBlobs restic.IDs, startOffset, endOffset uint, dstBlob restic.ID) error
type dictMatchFn func(srcBlobs restic.IDs, startOffset uint) (dstBlobs restic.IDs, numFinishedBlobs int, newOffset uint)

type fileChunkInfo struct {
	srcBlobs    restic.IDs
	blobPos     []uint        // file position of each blob's start
	seekBlobPos seekBlobPosFn // maps file position to blob position
	dictStore   dictStoreFn
	dictMatch   dictMatchFn
	prefixPos   uint
	prefixIdx   int
}

type PackedBlobLoader interface {
	LoadBlob(ctx context.Context, t restic.BlobType, id restic.ID, buf []byte) ([]byte, error)
	LoadBlobsFromPack(ctx context.Context, packID restic.ID, blobs []restic.Blob, handleBlobFn func(blob restic.BlobHandle, buf []byte, err error) error) error
}

type Rechunker struct {
	pol       chunker.Pol
	chunkDict *ChunkDict

	filesList    []chunkedFile
	blobSize     map[restic.ID]uint
	rechunkReady bool
	usePackCache bool

	// used if usePackCache == true
	blobToPack            map[restic.ID]restic.ID     // blob ID -> {blob length, pack ID}
	packToBlobs           map[restic.ID][]restic.Blob // pack ID -> list of blobs to be loaded from the pack
	sfPackToFiles         map[restic.ID][]chunkedFile // pack ID -> list of files{srcBlobIDs, hashOfIDs} that contains any blob in the pack (small files only)
	sfPackRequires        map[restic.ID]int           // hashOfIDs of srcBlobIDs -> number of packs until all blobs become ready in the cache (small files only)
	sfPackRequiresLock    sync.Mutex
	priorityFilesList     []chunkedFile
	priorityFilesListLock sync.Mutex

	// used in RewriteTree
	rechunkMap     map[restic.ID]restic.IDs // hashOfIDs of srcBlobIDs -> dstBlobIDs
	rewriteTreeMap map[restic.ID]restic.ID  // original tree ID (in src repo) -> rewritten tree ID (in dst repo)
	rechunkMapLock sync.Mutex
}

func NewRechunker(pol chunker.Pol) *Rechunker {
	return &Rechunker{
		pol:            pol,
		chunkDict:      NewChunkDict(),
		rechunkMap:     map[restic.ID]restic.IDs{},
		rewriteTreeMap: map[restic.ID]restic.ID{},
	}
}

const SMALL_FILE_THRESHOLD = 50
const LARGE_FILE_THRESHOLD = 50

func (rc *Rechunker) reset() {
	rc.filesList = nil
	rc.blobSize = map[restic.ID]uint{}
	rc.rechunkReady = false

	rc.blobToPack = map[restic.ID]restic.ID{}
	rc.packToBlobs = map[restic.ID][]restic.Blob{}
	rc.sfPackToFiles = map[restic.ID][]chunkedFile{}
	rc.sfPackRequires = map[restic.ID]int{}
}

func (rc *Rechunker) buildIndex(usePackCache bool, lookupBlobFn func(t restic.BlobType, id restic.ID) []restic.PackedBlob) error {
	// collect all required blobs
	allBlobs := restic.IDSet{}
	for _, file := range rc.filesList {
		for _, blob := range file.IDs {
			allBlobs.Insert(blob)
		}
	}

	// build blob lookup info
	for blob := range allBlobs {
		packs := lookupBlobFn(restic.DataBlob, blob)
		if len(packs) == 0 {
			return fmt.Errorf("can't find blob from source repo: %v", blob)
		}
		pb := packs[0]

		rc.blobSize[pb.Blob.ID] = pb.DataLength()
		rc.blobToPack[pb.Blob.ID] = pb.PackID
		rc.packToBlobs[pb.PackID] = append(rc.packToBlobs[pb.PackID], pb.Blob)
	}

	if !usePackCache { // nothing more to do
		return nil
	}

	// build file<->pack info for small files
	for _, file := range rc.filesList {
		if file.Len() >= SMALL_FILE_THRESHOLD {
			continue
		}
		packSet := restic.IDSet{}
		for _, blob := range file.IDs {
			pack := rc.blobToPack[blob]
			packSet.Insert(pack)
		}
		rc.sfPackRequires[file.hashval] = len(packSet)
		for p := range packSet {
			rc.sfPackToFiles[p] = append(rc.sfPackToFiles[p], file)
		}
	}

	return nil
}

func (rc *Rechunker) Plan(ctx context.Context, srcRepo restic.Repository, rootTrees []restic.ID, usePackCache bool) error {
	rc.reset()

	visitedFiles := map[restic.ID]struct{}{}
	visitedTrees := restic.IDSet{}

	// skip previously processed files and trees
	for k := range rc.rechunkMap {
		visitedFiles[k] = struct{}{}
	}
	for k := range rc.rewriteTreeMap {
		visitedTrees.Insert(k)
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	treeStream := data.StreamTrees(wgCtx, wg, srcRepo, rootTrees, func(id restic.ID) bool {
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

			// check if the tree blob is unstable json
			buf, err := json.Marshal(tree.Tree)
			if err != nil {
				return err
			}
			buf = append(buf, '\n')
			if tree.ID != restic.Hash(buf) {
				return fmt.Errorf("can't run rechunk-copy, because the following tree can't be rewritten without losing information:\n%v", tree.ID.String())
			}

			for _, node := range tree.Nodes {
				if node.Type == data.NodeTypeFile {
					hashval := hashOfIDs(node.Content)
					if _, ok := visitedFiles[hashval]; ok {
						continue
					}
					visitedFiles[hashval] = struct{}{}

					rc.filesList = append(rc.filesList, chunkedFile{
						node.Content,
						hashval,
					})
				}
			}
		}
		return nil
	})
	err := wg.Wait()
	if err != nil {
		return err
	}

	err = rc.buildIndex(usePackCache, srcRepo.LookupBlob)
	if err != nil {
		return err
	}

	// sort filesList by lexicographic pack order
	filesPacksMap := map[restic.ID][]byte{}
	maxFilePacksLength := 0
	maxFileLength := 0
	for _, file := range rc.filesList {
		var filePacks restic.IDs
		var currPack restic.ID
		for _, b := range file.IDs {
			p := rc.blobToPack[b]
			if currPack != p {
				currPack = p
				filePacks = append(filePacks, currPack)
			}
		}
		if maxFilePacksLength < filePacks.Len() {
			maxFilePacksLength = filePacks.Len()
			maxFileLength = file.Len()
		}
		filesPacksMap[file.hashval] = concatIDs(filePacks)
	}
	slices.SortFunc(rc.filesList, func(a, b chunkedFile) int {
		ap := filesPacksMap[a.hashval]
		bp := filesPacksMap[b.hashval]
		return bytes.Compare(ap, bp)
	})
	debug.Log("Max number of filePacks: %v, whose length is %v", maxFilePacksLength, maxFileLength)

	rc.usePackCache = usePackCache
	rc.rechunkReady = true

	return nil
}

func startFileStreamer(ctx context.Context, wg *errgroup.Group, srcBlobs restic.IDs, out chan<- fileStreamReader, getBlob getBlobFn, bufferPool chan []byte) {
	ch := make(chan []byte)

	// loader: load file chunks sequentially
	wg.Go(func() error {
		for i := 0; i < len(srcBlobs); i++ {
			// bring buffer from bufferPool
			var buf []byte
			select {
			case buf = <-bufferPool:
			default:
				buf = make([]byte, 0, chunker.MaxSize)
			}

			// get chunk data (may take a while)
			buf, err := getBlob(srcBlobs[i], buf)
			if err != nil {
				return err
			}

			// send the chunk to iopipe
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- buf:
			}
		}
		close(ch)
		return nil
	})

	// iopipe: convert chunks into io.Reader stream
	wg.Go(func() error {
		r, w := io.Pipe()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- fileStreamReader{r, 0}:
		}

		for {
			// receive chunk from loader
			var buf []byte
			var ok bool
			select {
			case <-ctx.Done():
				w.CloseWithError(ctx.Err())
				return ctx.Err()
			case buf, ok = <-ch:
				if !ok { // EOF
					err := w.Close()
					return err
				}
			}

			// stream-write through io.pipe
			_, err := w.Write(buf)
			if err != nil {
				w.CloseWithError(err)
				return err
			}

			// recycle used buffer into bufferPool
			select {
			case bufferPool <- buf:
			default:
			}
		}
	})
}

func startFileStreamerWithFastForward(ctx context.Context, wg *errgroup.Group, srcBlobs restic.IDs, out chan<- fileStreamReader, getBlob getBlobFn, bufferPool chan []byte, ff <-chan fastForward) {
	type blob struct {
		buf   []byte
		stNum int
	}
	ch := make(chan blob)

	// loader: load file chunks sequentially, with possible fast-forward (blob skipping)
	wg.Go(func() error {
		var stNum int
		var offset uint

	MainLoop:
		for i := 0; i < len(srcBlobs); i++ {
			// check if a fast-forward request has arrived
			select {
			case ffPos := <-ff:
				stNum = ffPos.newStNum
				i = ffPos.blobIdx
				offset = ffPos.offset
				if i >= len(srcBlobs) { // implies EOF
					break MainLoop
				}
			default:
			}

			// bring buffer from bufferPool
			var buf []byte
			select {
			case buf = <-bufferPool:
			default:
				buf = make([]byte, 0, chunker.MaxSize)
			}

			// get chunk data (may take a while)
			buf, err := getBlob(srcBlobs[i], buf)
			if err != nil {
				return err
			}
			if offset != 0 {
				copy(buf, buf[offset:])
				buf = buf[:len(buf)-int(offset)]
				offset = 0
			}

			// send the chunk to iopipe
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- blob{buf: buf, stNum: stNum}:
			}
		}
		close(ch)
		return nil
	})

	// iopipe: convert chunks into io.Reader stream
	wg.Go(func() error {
		var stNum int
		r, w := io.Pipe()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- fileStreamReader{r, 0}:
		}

		for {
			// receive chunk from loader
			var b blob
			var ok bool
			select {
			case <-ctx.Done():
				w.CloseWithError(ctx.Err())
				return ctx.Err()
			case b, ok = <-ch:
				if !ok { // EOF
					err := w.Close()
					return err
				}
			}

			// handle fast-forward
			if b.stNum > stNum {
				stNum = b.stNum
				w.CloseWithError(ErrNewStream)
				r, w = io.Pipe()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- fileStreamReader{r, stNum}:
				}
			}

			// stream-write through io.pipe
			buf := b.buf
			_, err := w.Write(buf)
			if err != nil {
				w.CloseWithError(err)
				return err
			}

			// recycle used buffer into bufferPool
			select {
			case bufferPool <- buf:
			default:
			}
		}
	})
}

func startChunker(ctx context.Context, wg *errgroup.Group, chnker *chunker.Chunker, pol chunker.Pol, in <-chan fileStreamReader, out chan<- chunk, bufferPool chan []byte) {
	wg.Go(func() error {
		var r fileStreamReader
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r = <-in:
		}
		chnker.Reset(r, pol)

		for {
			// bring buffer from bufferPool
			var buf []byte
			select {
			case buf = <-bufferPool:
			default:
				buf = make([]byte, 0, chunker.MaxSize)
			}

			// rechunk with new parameter
			c, err := chnker.Next(buf)
			if err == io.EOF { // reached EOF; all done
				select {
				case bufferPool <- buf:
				default:
				}
				close(out)
				return nil
			}
			if err == ErrNewStream { // fast-forward occurred; replace fileStreamReader
				select {
				case bufferPool <- buf:
				default:
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case r = <-in:
					chnker.Reset(r, pol)
				}
				continue
			}
			if err != nil {
				r.CloseWithError(err)
				return err
			}

			// send chunk to blob saver
			select {
			case <-ctx.Done():
				r.CloseWithError(ctx.Err())
				return ctx.Err()
			case out <- chunk{c, r.stNum}:
			}
		}
	})
}

func startFileBlobSaver(ctx context.Context, wg *errgroup.Group, in <-chan chunk, out chan<- restic.IDs, dstRepo restic.BlobSaver, bufferPool chan<- []byte) {
	wg.Go(func() error {
		dstBlobs := restic.IDs{}
		for {
			// receive chunk from chunker
			var c chunk
			var ok bool
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c, ok = <-in:
				if !ok { // EOF
					out <- dstBlobs
					return nil
				}
			}

			// save chunk to destination repo
			buf := c.Data
			dstBlobID, _, _, err := dstRepo.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			if err != nil {
				return err
			}

			// recycle used buffer into bufferPool
			select {
			case bufferPool <- buf:
			default:
			}
			dstBlobs = append(dstBlobs, dstBlobID)
		}
	})
}

func startFileBlobSaverWithFastForward(ctx context.Context, wg *errgroup.Group, in <-chan chunk, out chan<- restic.IDs, dstRepo restic.BlobSaver, bufferPool chan<- []byte, ff chan<- fastForward, info *fileChunkInfo) {
	wg.Go(func() error {
		var stNum int
		srcBlobs := info.srcBlobs
		blobPos := info.blobPos
		seekBlobPos := info.seekBlobPos
		dictStore := info.dictStore
		dictMatch := info.dictMatch
		currIdx := info.prefixIdx
		currPos := info.prefixPos
		dstBlobs := restic.IDs{}

		for {
			// receive chunk from chunker
			var c chunk
			var ok bool
			select {
			case <-ctx.Done():
				return ctx.Err()
			case c, ok = <-in:
				if !ok { // EOF
					out <- dstBlobs
					return nil
				}
			}

			if c.stNum < stNum {
				// just arrived chunk had been skipped by chunkDict match,
				// so just flush it away and receive next chunk
				select {
				case bufferPool <- c.Data:
				default:
				}
				continue
			}

			// save chunk to destination repo
			buf := c.Data
			dstBlobID, _, _, err := dstRepo.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			if err != nil {
				return err
			}

			startOffset := currPos - blobPos[currIdx]
			endPos := currPos + c.Length
			endIdx, endOffset := seekBlobPos(endPos, currIdx)

			// slice srcBlobs which corresponds to current chunk into chunkSrcBlobs
			var chunkSrcBlobs restic.IDs
			if endIdx == len(srcBlobs) { // tail-of-file chunk
				// last element of chunkSrcBlobs should be nullID, which indicates EOF
				chunkSrcBlobs = make(restic.IDs, endIdx-currIdx+1)
				n := copy(chunkSrcBlobs, srcBlobs[currIdx:endIdx])
				if n != endIdx-currIdx {
					panic("srcBlobs slice copy error")
				}
			} else { // mid-file chunk
				chunkSrcBlobs = srcBlobs[currIdx : endIdx+1]
			}

			// store chunk mapping to ChunkDict
			err = dictStore(chunkSrcBlobs, startOffset, endOffset, dstBlobID)
			if err != nil {
				return err
			}

			// update current position in a file
			currPos = endPos
			currIdx = endIdx
			currOffset := endOffset

			// recycle used buffer into bufferPool
			select {
			case bufferPool <- buf:
			default:
			}
			dstBlobs = append(dstBlobs, dstBlobID)

			// match chunks from ChunkDict
			matchedDstBlobs, numFinishedSrcBlobs, newOffset := dictMatch(srcBlobs[currIdx:], currOffset)
			if numFinishedSrcBlobs > 4 { // apply only when you can skip many blobs; otherwise, it would be better not to interrupt the pipeline
				// debug trace
				debug.Log("ChunkDict match at %v: Skipping %d blobs", srcBlobs[currIdx].Str(), numFinishedSrcBlobs)
				debugNoteLock.Lock()
				debugNote["chunkdict_event"]++
				debugNote["chunkdict_blob_count"] += numFinishedSrcBlobs
				debugNoteLock.Unlock()

				dstBlobs = append(dstBlobs, matchedDstBlobs...)

				currIdx += numFinishedSrcBlobs
				currPos = blobPos[currIdx] + newOffset

				stNum++
				ff <- fastForward{
					newStNum: stNum,
					blobIdx:  currIdx,
					offset:   newOffset,
				}
			}
		}
	})
}

func (rc *Rechunker) newCache(ctx context.Context, wg *errgroup.Group, srcRepo PackedBlobLoader, numDownloaders int) *PackCache {
	return NewPackCache(ctx, wg, rc.blobToPack, numDownloaders, func(packID restic.ID) (BlobsMap, error) {
		// downloadFn implementation
		blobData := BlobsMap{}
		blobs := rc.packToBlobs[packID]
		err := srcRepo.LoadBlobsFromPack(ctx, packID, blobs,
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
			return BlobsMap{}, err
		}
		return blobData, nil
	}, func(packID restic.ID) {
		// debug trace
		debug.Log("Pack %v loaded", packID.Str())
		debugNoteLock.Lock()
		debugNote["load:"+packID.String()]++
		debugNoteLock.Unlock()

		// onPackReady implementation
		filesToUpdate := rc.sfPackToFiles[packID]
		var readyFiles []chunkedFile

		rc.sfPackRequiresLock.Lock()
		for _, file := range filesToUpdate {
			if rc.sfPackRequires[file.hashval] > 0 {
				rc.sfPackRequires[file.hashval]--
				if rc.sfPackRequires[file.hashval] == 0 {
					readyFiles = append(readyFiles, file)
				}
			}
		}
		rc.sfPackRequiresLock.Unlock()

		rc.priorityFilesListLock.Lock()
		rc.priorityFilesList = append(rc.priorityFilesList, readyFiles...)
		rc.priorityFilesListLock.Unlock()
	}, func(packID restic.ID) {
		// debug trace
		debug.Log("Pack %v evicted", packID.Str())
		debugNoteLock.Lock()
		debugNote["evict:"+packID.String()]++
		debugNoteLock.Unlock()

		// onPackEvict implementation
		filesToUpdate := rc.sfPackToFiles[packID]
		rc.sfPackRequiresLock.Lock()
		for _, file := range filesToUpdate {
			// files with sPackRequires==0 has already gone to priorityFilesList, so don't track them
			if rc.sfPackRequires[file.hashval] > 0 {
				rc.sfPackRequires[file.hashval]++
			}
		}
		rc.sfPackRequiresLock.Unlock()
	})
}

func (rc *Rechunker) runDispatcher(ctx context.Context, wg *errgroup.Group) chan restic.IDs {
	chDispatch := make(chan restic.IDs)
	if rc.usePackCache {
		wg.Go(func() error {
			seenFiles := map[restic.ID]struct{}{}
			regularTrack := rc.filesList
			var fastTrack []chunkedFile

			for {
				if len(fastTrack) == 0 {
					rc.priorityFilesListLock.Lock()
					if len(rc.priorityFilesList) > 0 {
						fastTrack = rc.priorityFilesList
						rc.priorityFilesList = nil
					}
					rc.priorityFilesListLock.Unlock()
				}

				if len(fastTrack) > 0 {
					file := fastTrack[0]
					fastTrack = fastTrack[1:]
					if _, ok := seenFiles[file.hashval]; ok {
						continue
					}
					seenFiles[file.hashval] = struct{}{}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case chDispatch <- file.IDs:
					}
				} else if len(regularTrack) > 0 {
					file := regularTrack[0]
					regularTrack = regularTrack[1:]
					if _, ok := seenFiles[file.hashval]; ok {
						continue
					}
					seenFiles[file.hashval] = struct{}{}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case chDispatch <- file.IDs:
					}
				} else { // no more jobs
					close(chDispatch)
					return nil
				}
			}
		})
	} else {
		wg.Go(func() error {
			for _, file := range rc.filesList {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chDispatch <- file.IDs:
				}
			}
			close(chDispatch)
			return nil
		})
	}

	return chDispatch
}

func (rc *Rechunker) runWorkers(ctx context.Context, wg *errgroup.Group, numWorkers int, getBlob getBlobFn, dstRepo restic.BlobSaver, chDispatch <-chan restic.IDs, p *progress.Counter) {
	bufferPool := make(chan []byte, 4*numWorkers)

	for range numWorkers {
		wg.Go(func() error {
			chnker := chunker.New(nil, rc.pol)

			for {
				var srcBlobs restic.IDs
				var ok bool
				select {
				case <-ctx.Done():
					return ctx.Err()
				case srcBlobs, ok = <-chDispatch:
					if !ok { // all files finished and chan closed
						return nil
					}
				}
				dstBlobs := restic.IDs{}

				chStreamer := make(chan fileStreamReader)
				chChunk := make(chan chunk)
				chFastForward := make(chan fastForward, 1)
				wgIn, ctxIn := errgroup.WithContext(ctx)

				// data preparation for ChunkDict
				useChunkDict := len(srcBlobs) != 0 && len(srcBlobs) >= LARGE_FILE_THRESHOLD
				var info *fileChunkInfo
				if useChunkDict {
					var blobPos []uint
					var seekBlobPos func(uint, int) (int, uint)
					var prefixPos uint
					var prefixIdx int

					// build blobPos (position of each blob in a file)
					blobPos = make([]uint, len(srcBlobs)+1)
					var offset uint
					for i, blob := range srcBlobs {
						offset += rc.blobSize[blob]
						blobPos[i+1] = offset
					}
					if blobPos[1] == 0 { // assertion
						panic("blobPos not computed correctly")
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
					prefixBlobs, numFinishedBlobs, newOffset := rc.chunkDict.Match(srcBlobs, 0)
					if numFinishedBlobs > 0 {
						// debug trace
						debug.Log("ChunkDict match at %v (prefix): Skipping %d blobs", srcBlobs[0].Str(), numFinishedBlobs)
						debugNoteLock.Lock()
						debugNote["chunkdict_event"]++
						debugNote["chunkdict_blob_count"] += numFinishedBlobs
						debugNoteLock.Unlock()

						prefixIdx = numFinishedBlobs
						prefixPos = blobPos[numFinishedBlobs] + newOffset
						dstBlobs = prefixBlobs

						chFastForward <- fastForward{
							newStNum: 0,
							blobIdx:  numFinishedBlobs,
							offset:   newOffset,
						}
					}

					info = &fileChunkInfo{
						srcBlobs:    srcBlobs,
						blobPos:     blobPos,
						seekBlobPos: seekBlobPos,
						dictStore:   rc.chunkDict.Store,
						dictMatch:   rc.chunkDict.Match,
						prefixPos:   prefixPos,
						prefixIdx:   prefixIdx,
					}
				}

				chDstBlobs := make(chan restic.IDs, 1)

				if useChunkDict {
					startFileStreamerWithFastForward(ctxIn, wgIn, srcBlobs, chStreamer, getBlob, bufferPool, chFastForward)
					startChunker(ctxIn, wgIn, chnker, rc.pol, chStreamer, chChunk, bufferPool)
					startFileBlobSaverWithFastForward(ctxIn, wgIn, chChunk, chDstBlobs, dstRepo, bufferPool, chFastForward, info)
				} else {
					startFileStreamer(ctxIn, wgIn, srcBlobs, chStreamer, getBlob, bufferPool)
					startChunker(ctxIn, wgIn, chnker, rc.pol, chStreamer, chChunk, bufferPool)
					startFileBlobSaver(ctxIn, wgIn, chChunk, chDstBlobs, dstRepo, bufferPool)
				}

				err := wgIn.Wait()
				if err != nil {
					return err
				}

				dstBlobs = append(dstBlobs, <-chDstBlobs...)

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

}

func (rc *Rechunker) RechunkData(ctx context.Context, srcRepo PackedBlobLoader, dstRepo restic.BlobSaver, p *progress.Counter) error {
	if !rc.rechunkReady {
		return fmt.Errorf("Plan() must be run first before RechunkData()")
	}
	rc.rechunkReady = false

	wgBg, wgBgCtx := errgroup.WithContext(ctx)
	wgFg, wgFgCtx := errgroup.WithContext(ctx)
	numWorkers := runtime.GOMAXPROCS(0)

	// pack cache
	var cache *PackCache
	var getBlob getBlobFn
	numDownloaders := min(numWorkers, 4)

	if rc.usePackCache {
		cache = rc.newCache(wgBgCtx, wgBg, srcRepo, numDownloaders)
		// implement getBlob using blob cache
		getBlob = func(blobID restic.ID, buf []byte) ([]byte, error) {
			blob, ch := cache.Get(wgFgCtx, wgFg, blobID, buf)
			if blob == nil { // wait for blob to be downloaded
				select {
				case <-wgFgCtx.Done():
					return nil, wgFgCtx.Err()
				case blob = <-ch:
				}
			}
			return blob, nil
		}
	} else {
		getBlob = func(blobID restic.ID, buf []byte) ([]byte, error) {
			return srcRepo.LoadBlob(wgFgCtx, restic.DataBlob, blobID, buf)
		}
	}

	// run job dispatcher
	chDispatch := rc.runDispatcher(wgBgCtx, wgBg)

	// run workers
	rc.runWorkers(wgFgCtx, wgFg, numWorkers, getBlob, dstRepo, chDispatch, p)

	// wait for foreground workers to finish
	err := wgFg.Wait()
	if err != nil {
		return err
	}
	// shutdown background workers
	if rc.usePackCache {
		cache.Close()
	}
	err = wgBg.Wait()
	if err != nil {
		return err
	}

	// debug trace: print report
	if rc.usePackCache {
		debug.Log("List of packs downloaded more than once:")
		numPackRedundant := 0
		redundantDownloadCount := 0
		for k := range debugNote {
			if strings.HasPrefix(k, "load:") && debugNote[k] > 1 {
				debug.Log("%v: Downloaded %d times, evicted %d times", k[5:15], debugNote[k], debugNote["evict:"+k[5:]])
				numPackRedundant++
				redundantDownloadCount += debugNote[k]
			}
		}
		debug.Log("[summary_packcache] Number of redundantly downloaded packs is %d, whose overall download count is %d", numPackRedundant, redundantDownloadCount)
	}
	debug.Log("[summary_chunkdict] ChunkDict match happend %d times, saving %d blob processings", debugNote["chunkdict_event"], debugNote["chunkdict_blob_count"])

	return nil
}

func (rc *Rechunker) rewriteNode(node *data.Node) error {
	if node.Type != data.NodeTypeFile {
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

	curTree, err := data.LoadTree(ctx, srcRepo, nodeID)
	if err != nil {
		return restic.ID{}, err
	}

	tb := data.NewTreeJSONBuilder()
	for _, node := range curTree.Nodes {
		if ctx.Err() != nil {
			return restic.ID{}, ctx.Err()
		}

		err = rc.rewriteNode(node)
		if err != nil {
			return restic.ID{}, err
		}

		if node.Type != data.NodeTypeDir {
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

func hashOfIDs(ids restic.IDs) restic.ID {
	c := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		c = append(c, id[:]...)
	}
	return sha256.Sum256(c)
}

func concatIDs(ids restic.IDs) []byte {
	c := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		c = append(c, id[:]...)
	}
	return c
}
