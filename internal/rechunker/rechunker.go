package rechunker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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

const SMALL_FILE_THRESHOLD = 50
const LARGE_FILE_THRESHOLD = 50

// data structure for debug trace
var debugNote = map[string]int{}
var debugNoteLock = sync.Mutex{}

type getBlobFn func(blobID restic.ID, buf []byte, prefetch restic.IDs) ([]byte, error)
type seekBlobPosFn func(pos uint, seekStartIdx int) (idx int, offset uint)                                               // given pos in a file, find blob idx and offset
type dictStoreFn func(srcBlobs restic.IDs, startOffset, endOffset uint, dstBlob restic.ID) error                         // store a blob mapping to ChunkDict
type dictMatchFn func(srcBlobs restic.IDs, startOffset uint) (dstBlobs restic.IDs, numFinishedBlobs int, newOffset uint) // get matching blob mapping from ChunkDict

// ChunkedFileContext has variables and functions needed for use in rechunk workers with ChunkDict.
type ChunkedFileContext struct {
	srcBlobs    restic.IDs
	blobPos     []uint        // file position of each blob's start
	seekBlobPos seekBlobPosFn // maps file position to blob position
	dictStore   dictStoreFn
	dictMatch   dictMatchFn
	prefixPos   uint
	prefixIdx   int
}

// PriorityFilesHandler is a wrapper for priority files (which are readily available in the blob cache).
type PriorityFilesHandler struct {
	filesList []*ChunkedFile
	mu        sync.Mutex
	arrival   chan struct{} // should be closed iff filesList != nil

	done chan struct{}
}

func NewPriorityFilesHandler() *PriorityFilesHandler {
	return &PriorityFilesHandler{
		arrival: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (h *PriorityFilesHandler) Push(files []*ChunkedFile) bool {
	select {
	case <-h.done:
		return false
	default:
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	listWasNil := (h.filesList == nil)
	h.filesList = append(h.filesList, files...)
	if listWasNil && h.filesList != nil {
		close(h.arrival)
	}

	return true
}

func (h *PriorityFilesHandler) Arrival() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.arrival
}

func (h *PriorityFilesHandler) Pop() []*ChunkedFile {
	select {
	case <-h.done:
		return nil
	default:
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.filesList == nil {
		return nil
	}
	l := h.filesList
	h.filesList = nil
	h.arrival = make(chan struct{})
	return l
}

func (h *PriorityFilesHandler) Close() {
	if h == nil {
		return
	}

	select {
	case <-h.done:
	default:
		close(h.done)
	}
}

type Rechunker struct {
	pol                  chunker.Pol
	chunkDict            *ChunkDict
	cache                *BlobCache
	priorityFilesHandler *PriorityFilesHandler

	filesList    []*ChunkedFile
	rechunkReady bool

	rechunkMap     map[restic.ID]restic.IDs // hashOfIDs of srcBlobIDs -> dstBlobIDs
	rechunkMapLock sync.Mutex
	rewriteTreeMap map[restic.ID]restic.ID // original tree ID (in src repo) -> rewritten tree ID (in dst repo)

	// static data: once computed in Plan()
	blobSize      map[restic.ID]uint
	blobToPack    map[restic.ID]restic.ID      // blob ID -> {blob length, pack ID}
	packToBlobs   map[restic.ID][]restic.Blob  // pack ID -> list of blobs to be loaded from the pack
	sfBlobToFiles map[restic.ID][]*ChunkedFile // pack ID -> list of files{srcBlobIDs, hashOfIDs} that contains any blob in the pack (small files only)

	// dynamic states
	sfBlobRequires     map[restic.ID]int // hashOfIDs of srcBlobIDs -> number of packs until all blobs become ready in the cache (small files only)
	sfBlobRequiresLock sync.Mutex
	blobRemaining      map[restic.ID]int
	blobRemainingLock  sync.Mutex
}

func NewRechunker(pol chunker.Pol) *Rechunker {
	return &Rechunker{
		pol:            pol,
		chunkDict:      NewChunkDict(),
		rechunkMap:     map[restic.ID]restic.IDs{},
		rewriteTreeMap: map[restic.ID]restic.ID{},
	}
}

func (rc *Rechunker) reset() {
	rc.cache = nil
	rc.priorityFilesHandler = nil

	rc.filesList = nil
	rc.rechunkReady = false

	rc.blobSize = map[restic.ID]uint{}
	rc.blobToPack = map[restic.ID]restic.ID{}
	rc.packToBlobs = map[restic.ID][]restic.Blob{}
	rc.sfBlobToFiles = map[restic.ID][]*ChunkedFile{}

	rc.sfBlobRequires = map[restic.ID]int{}
	rc.blobRemaining = map[restic.ID]int{}
}

func (rc *Rechunker) buildIndex(useBlobCache bool, lookupBlobFn func(t restic.BlobType, id restic.ID) []restic.PackedBlob) error {
	// collect blob usage info
	for _, file := range rc.filesList {
		for _, blob := range file.IDs {
			rc.blobRemaining[blob]++
		}
	}

	// build blob lookup info
	for blob := range rc.blobRemaining {
		packs := lookupBlobFn(restic.DataBlob, blob)
		if len(packs) == 0 {
			return fmt.Errorf("can't find blob from source repo: %v", blob)
		}
		pb := packs[0]

		rc.blobSize[pb.Blob.ID] = pb.DataLength()
		rc.blobToPack[pb.Blob.ID] = pb.PackID
		rc.packToBlobs[pb.PackID] = append(rc.packToBlobs[pb.PackID], pb.Blob)
	}

	if !useBlobCache { // nothing more to do
		return nil
	}

	// build blob trace info for small files
	for _, file := range rc.filesList {
		if file.Len() >= SMALL_FILE_THRESHOLD {
			continue
		}
		blobSet := restic.NewIDSet(file.IDs...)
		rc.sfBlobRequires[file.hashval] = len(blobSet)
		for b := range blobSet {
			rc.sfBlobToFiles[b] = append(rc.sfBlobToFiles[b], file)
		}
	}

	return nil
}

func (rc *Rechunker) Plan(ctx context.Context, srcRepo restic.Repository, rootTrees []restic.ID, useBlobCache bool) error {
	rc.reset()

	visitedFiles := restic.IDSet{}
	visitedTrees := restic.IDSet{}

	// skip previously processed files and trees
	for k := range rc.rechunkMap {
		visitedFiles.Insert(k)
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
					hashval := HashOfIDs(node.Content)
					if visitedFiles.Has(hashval) {
						continue
					}
					visitedFiles.Insert(hashval)

					rc.filesList = append(rc.filesList, &ChunkedFile{
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

	err = rc.buildIndex(useBlobCache, srcRepo.LookupBlob)
	if err != nil {
		return err
	}

	// sort filesList by length (descending order)
	slices.SortFunc(rc.filesList, func(a, b *ChunkedFile) int {
		return len(b.IDs) - len(a.IDs) // descending order
	})

	rc.rechunkReady = true

	return nil
}

func (rc *Rechunker) runCache(ctx context.Context, wg *errgroup.Group, srcRepo restic.Repository, numDownloaders int, cacheSize int) {
	rc.priorityFilesHandler = NewPriorityFilesHandler()

	rc.cache = NewBlobCache(ctx, wg, cacheSize, numDownloaders, rc.blobToPack, rc.packToBlobs, srcRepo,
		func(blobIDs restic.IDs) {
			// onReady implementation
			var readyFiles []*ChunkedFile

			rc.sfBlobRequiresLock.Lock()
			for _, id := range blobIDs {
				filesToUpdate := rc.sfBlobToFiles[id]

				for _, file := range filesToUpdate {
					if rc.sfBlobRequires[file.hashval] > 0 {
						rc.sfBlobRequires[file.hashval]--
						if rc.sfBlobRequires[file.hashval] == 0 {
							readyFiles = append(readyFiles, file)
						}
					}
				}
			}
			rc.sfBlobRequiresLock.Unlock()

			_ = rc.priorityFilesHandler.Push(readyFiles)

			// debug trace
			debugNoteLock.Lock()
			for _, id := range blobIDs {
				debugNote["load:"+id.String()]++
			}
			debugNoteLock.Unlock()
		}, func(blobIDs restic.IDs) {
			// onEvict implementation
			rc.sfBlobRequiresLock.Lock()
			for _, id := range blobIDs {
				filesToUpdate := rc.sfBlobToFiles[id]
				for _, file := range filesToUpdate {
					// files with sPackRequires==0 has already gone to priorityFilesList, so don't track them
					if rc.sfBlobRequires[file.hashval] > 0 {
						rc.sfBlobRequires[file.hashval]++
					}
				}
			}
			rc.sfBlobRequiresLock.Unlock()
		})
}

func (rc *Rechunker) runDispatcher(ctx context.Context, wg *errgroup.Group) (chRegular, chPriority chan *ChunkedFile) {
	if rc.cache != nil {
		chRegular = make(chan *ChunkedFile)
		chPriority = make(chan *ChunkedFile)

		seenFiles := restic.IDSet{}
		seenFilesLock := sync.Mutex{}

		// goroutine for priority track (chPriority)
		wg.Go(func() error {
			defer close(chPriority)

			var priorityTrack []*ChunkedFile
			for {
				if len(priorityTrack) == 0 {
					// wait for priority files arrival
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-rc.priorityFilesHandler.done:
						return nil
					case <-rc.priorityFilesHandler.Arrival():
						priorityTrack = rc.priorityFilesHandler.Pop()
						continue
					}
				}

				file := priorityTrack[0]
				priorityTrack = priorityTrack[1:]

				// check if the file was handled by another dispatcher;
				// if it was, skip the file.
				seenFilesLock.Lock()
				seen := seenFiles.Has(file.hashval)
				if !seen {
					seenFiles.Insert(file.hashval)
				}
				seenFilesLock.Unlock()
				if seen {
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-rc.priorityFilesHandler.done:
					return nil
				case chPriority <- file:
				}
			}
		})

		// goroutine for regular track (chRegular)
		wg.Go(func() error {
			filesList := rc.filesList
			for _, file := range filesList {
				// check if the file was handled by another dispatcher;
				// if it was, skip the file.
				seenFilesLock.Lock()
				seen := seenFiles.Has(file.hashval)
				if !seen {
					seenFiles.Insert(file.hashval)
				}
				seenFilesLock.Unlock()
				if seen {
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case chRegular <- file:
				}
			}
			close(chRegular)
			rc.priorityFilesHandler.Close()
			return nil
		})
	} else {
		chRegular = make(chan *ChunkedFile)

		wg.Go(func() error {
			for _, file := range rc.filesList {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chRegular <- file:
				}
			}
			close(chRegular)
			return nil
		})
	}

	return chRegular, chPriority
}

func (rc *Rechunker) runWorkers(ctx context.Context, wg *errgroup.Group, numWorkers int, getBlob getBlobFn, dstRepo restic.BlobSaver, chFirst <-chan *ChunkedFile, chSecond <-chan *ChunkedFile, p *progress.Counter) {
	if chFirst == nil { // assertion
		panic("chFirst must not be nil")
	}

	bufferPool := make(chan []byte, 4*numWorkers)

	for range numWorkers {
		wg.Go(func() error {
			chnker := chunker.New(nil, rc.pol)

			for {
				var file *ChunkedFile
				var ok bool

				if chSecond != nil {
					// Firstly, try chFirst only. If chFirst is not ready now, wait for both chFirst and chSecond.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case file, ok = <-chFirst:
					default:
						select {
						case <-ctx.Done():
							return ctx.Err()
						case file, ok = <-chFirst:
						case file, ok = <-chSecond:
						}
					}
				} else {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case file, ok = <-chFirst:
					}
				}

				if !ok { // all files finished and chan closed
					return nil
				}

				srcBlobs := file.IDs
				dstBlobs := restic.IDs{}

				chStreamer := make(chan fileStreamReader)
				chChunk := make(chan chunk)
				chFastForward := make(chan fastForward, 1)
				wgIn, ctxIn := errgroup.WithContext(ctx)

				// data preparation for ChunkDict
				useChunkDict := len(srcBlobs) != 0 && len(srcBlobs) >= LARGE_FILE_THRESHOLD
				var info *ChunkedFileContext
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

					info = &ChunkedFileContext{
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
				hashval := HashOfIDs(srcBlobs)
				rc.rechunkMapLock.Lock()
				rc.rechunkMap[hashval] = dstBlobs
				rc.rechunkMapLock.Unlock()

				if p != nil {
					p.Add(1)
				}

				err = rc.fileComplete(ctx, srcBlobs)
				if err != nil {
					return err
				}
			}
		})
	}

}

func (rc *Rechunker) fileComplete(ctx context.Context, srcBlobs restic.IDs) error {
	if rc.cache == nil {
		return nil
	}

	var ignoresList restic.IDs
	rc.blobRemainingLock.Lock()
	for _, blob := range srcBlobs {
		rc.blobRemaining[blob]--
		if rc.blobRemaining[blob] == 0 {
			ignoresList = append(ignoresList, blob)
		}
	}
	rc.blobRemainingLock.Unlock()

	if len(ignoresList) > 0 {
		return rc.cache.Ignore(ctx, ignoresList)
	}
	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, srcRepo restic.Repository, dstRepo restic.BlobSaver, cacheSize int, p *progress.Counter) error {
	if !rc.rechunkReady {
		return fmt.Errorf("Plan() must be run first before RechunkData()")
	}
	rc.rechunkReady = false

	wgBg, wgBgCtx := errgroup.WithContext(ctx)
	wgFg, wgFgCtx := errgroup.WithContext(ctx)
	numWorkers := runtime.GOMAXPROCS(0)

	// blob cache
	var getBlob getBlobFn
	if cacheSize > 0 {
		debug.Log("Creating blob cache: cacheSize %v", cacheSize)
		numDownloaders := min(numWorkers, int(srcRepo.Connections()))
		rc.runCache(wgBgCtx, wgBg, srcRepo, numDownloaders, cacheSize)
		// implement getBlob with the blob cache as an intermediary
		getBlob = func(blobID restic.ID, buf []byte, prefetch restic.IDs) ([]byte, error) {
			blob, ch := rc.cache.Get(wgFgCtx, wgFg, blobID, buf, prefetch)
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
		// if the blob cache is disabled, getblob directly uses srcRepo's LoadBlob()
		getBlob = func(blobID restic.ID, buf []byte, _ restic.IDs) ([]byte, error) {
			return srcRepo.LoadBlob(wgFgCtx, restic.DataBlob, blobID, buf)
		}
	}

	// run job dispatcher
	debug.Log("Running job dispatcher")
	// if the blob cache is enabled, both chRegular and chPriority are used.
	// if the blob cache is disabled, only chRegular is used.
	chRegular, chPriority := rc.runDispatcher(wgBgCtx, wgBg)

	// run workers
	debug.Log("Running rechunk workers")
	if rc.cache != nil {
		rc.runWorkers(wgFgCtx, wgFg, numWorkers, getBlob, dstRepo, chPriority, chRegular, p)
		rc.runWorkers(wgFgCtx, wgFg, 1, getBlob, dstRepo, chPriority, nil, p) // a dedicated worker that only processes priority files
	} else {
		rc.runWorkers(wgFgCtx, wgFg, numWorkers, getBlob, dstRepo, chRegular, nil, p)
	}

	// wait for foreground workers to finish
	err := wgFg.Wait()
	if err != nil {
		return err
	}
	debug.Log("All rechunk workers finished.")

	// finish background workers
	rc.cache.Close()
	err = wgBg.Wait()
	if err != nil {
		return err
	}
	debug.Log("All background workers finished.")

	// debug trace: print report
	if rc.cache != nil {
		debug.Log("List of blobs downloaded more than once:")
		numBlobRedundant := 0
		redundantDownloadCount := 0
		for k := range debugNote {
			if strings.HasPrefix(k, "load:") && debugNote[k] > 1 {
				debug.Log("%v: Downloaded %d times", k[5:15], debugNote[k])
				numBlobRedundant++
				redundantDownloadCount += debugNote[k]
			}
		}
		debug.Log("[summary_blobcache] Number of redundantly downloaded blobs is %d, whose overall download count is %d", numBlobRedundant, redundantDownloadCount)
	}
	debug.Log("[summary_chunkdict] ChunkDict match happend %d times, saving %d blob processings", debugNote["chunkdict_event"], debugNote["chunkdict_blob_count"])

	return nil
}

func (rc *Rechunker) rewriteNode(node *data.Node) error {
	if node.Type != data.NodeTypeFile {
		return nil
	}

	hashval := HashOfIDs(node.Content)
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

func HashOfIDs(ids restic.IDs) restic.ID {
	c := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		c = append(c, id[:]...)
	}
	return sha256.Sum256(c)
}
