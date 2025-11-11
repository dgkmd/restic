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

type Rechunker struct {
	pol                  chunker.Pol
	chunkDict            *ChunkDict
	cache                *BlobCache
	priorityFilesHandler *PriorityFilesHandler

	filesList    []*ChunkedFile
	rechunkReady bool

	idx *RechunkerIndex

	rechunkMap     map[restic.ID]restic.IDs // hashOfIDs of srcBlobIDs -> dstBlobIDs
	rechunkMapLock sync.Mutex
	rewriteTreeMap map[restic.ID]restic.ID // original tree ID (in src repo) -> rewritten tree ID (in dst repo)
}

type RechunkerIndex struct {
	// static data: once computed in Plan()
	blobSize      map[restic.ID]uint
	blobToPack    map[restic.ID]restic.ID      // blob ID -> {blob length, pack ID}
	packToBlobs   map[restic.ID][]restic.Blob  // pack ID -> list of blobs to be loaded from the pack
	sfBlobToFiles map[restic.ID][]*ChunkedFile // pack ID -> list of files{srcBlobIDs, hashOfIDs} that contains any blob in the pack (small files only)

	// dynamic states
	blobRemaining      map[restic.ID]int
	blobRemainingLock  sync.Mutex
	sfBlobRequires     map[restic.ID]int // hashOfIDs of srcBlobIDs -> number of packs until all blobs become ready in the cache (small files only)
	sfBlobRequiresLock sync.Mutex
}

func NewRechunker(pol chunker.Pol) *Rechunker {
	return &Rechunker{
		pol:            pol,
		chunkDict:      NewChunkDict(),
		rechunkMap:     map[restic.ID]restic.IDs{},
		rewriteTreeMap: map[restic.ID]restic.ID{},
	}
}

// TODO: compute stats info and return it (number of snapshots, number of packs, total size, ...)
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

	var err error
	debug.Log("Gathering distinct file Contents from target snapshots")
	rc.filesList, err = gatherFileContents(ctx, srcRepo, rootTrees, visitedFiles, visitedTrees)
	if err != nil {
		return err
	}

	debug.Log("Building the internal index for use in RechunkData()")
	rc.idx, err = createIndex(rc.filesList, srcRepo.LookupBlob, useBlobCache)
	if err != nil {
		return err
	}

	debug.Log("Sorting the file list by their chunk counts (descending order)")
	slices.SortFunc(rc.filesList, func(a, b *ChunkedFile) int {
		return len(b.IDs) - len(a.IDs) // descending order
	})

	// TODO: compute statistics

	rc.rechunkReady = true

	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, srcRepo restic.Repository, dstRepo restic.BlobSaver, cacheSize int, p *progress.Counter) error {
	if !rc.rechunkReady {
		return fmt.Errorf("Plan() must be run first before RechunkData()")
	}
	rc.rechunkReady = false

	wgBg, wgBgCtx := errgroup.WithContext(ctx) // workgroup for background workers (cache, dispatchers)
	wgFg, wgFgCtx := errgroup.WithContext(ctx) // workgroup for foreground workers (rechunk workers, blob downloaders)
	numWorkers := min(runtime.GOMAXPROCS(0), int(srcRepo.Connections()))
	debug.Log("srcRepo.Connections(): %v", srcRepo.Connections())

	// prepare blob cache and getBlob function
	var getBlob getBlobFn
	if cacheSize > 0 {
		debug.Log("Creating blob cache: cacheSize %v", cacheSize)
		numDownloaders := numWorkers
		rc.cache, rc.priorityFilesHandler = startCache(wgBgCtx, wgBg, srcRepo, rc.idx, numDownloaders, cacheSize)
		// define getBlob with the blob cache as an intermediary
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

	debug.Log("Running job dispatcher")
	// if the blob cache is enabled, both chRegular and chPriority are used.
	// if the blob cache is disabled, only chRegular is used.
	chRegular, chPriority := startDispatcher(wgBgCtx, wgBg, rc.filesList, rc.cache, rc.priorityFilesHandler)

	// run workers
	debug.Log("Running rechunk workers")
	onBlobLoad := createBlobLoadCallback(ctx, rc.cache, rc.idx)
	if rc.cache != nil {
		rc.runWorkers(wgFgCtx, wgFg, numWorkers, getBlob, dstRepo, chPriority, chRegular, onBlobLoad, p)
		rc.runWorkers(wgFgCtx, wgFg, 1, getBlob, dstRepo, chPriority, nil, onBlobLoad, p) // a dedicated worker that only processes priority files
	} else {
		rc.runWorkers(wgFgCtx, wgFg, numWorkers, getBlob, dstRepo, chRegular, nil, onBlobLoad, p)
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

	// debugNote: print report
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
		debug.Log("[summary_blobcache] Max memory usage by cache: %v/%v bytes", debugNote["max_cache_usage"], cacheSize)
	}
	debug.Log("[summary_chunkdict] ChunkDict match happend %d times, saving %d blob processings", debugNote["chunkdict_event"], debugNote["chunkdict_blob_count"])

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

		// if the node is non-directory node, add it to the tree
		if node.Type != data.NodeTypeDir {
			err = tb.AddNode(node)
			if err != nil {
				return restic.ID{}, err
			}
			continue
		}

		// if the node is directory node, rewrite it recursively
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

	// save new tree to the destination repo
	newTreeID, _, _, err := dstRepo.SaveBlob(ctx, restic.TreeBlob, tree, restic.ID{}, false)
	rc.rewriteTreeMap[nodeID] = newTreeID
	return newTreeID, err
}

func (rc *Rechunker) reset() {
	rc.cache = nil
	rc.priorityFilesHandler = nil

	rc.filesList = nil
	rc.rechunkReady = false

	rc.idx = nil
}

func gatherFileContents(ctx context.Context, srcRepo restic.Repository, rootTrees restic.IDs, visitedFiles restic.IDSet, visitedTrees restic.IDSet) (filesList []*ChunkedFile, err error) {
	wg, wgCtx := errgroup.WithContext(ctx)

	// create StreamTrees channel that streams through all subtrees in target snapshots
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
				// you only have to rechunk regular files; so skip other file types
				if node.Type == data.NodeTypeFile {
					hashval := HashOfIDs(node.Content)
					if visitedFiles.Has(hashval) {
						continue
					}
					visitedFiles.Insert(hashval)

					filesList = append(filesList, &ChunkedFile{
						node.Content,
						hashval,
					})
				}
			}
		}
		return nil
	})
	err = wg.Wait()
	if err != nil {
		return nil, err
	}
	return filesList, nil
}

func createIndex(filesList []*ChunkedFile, lookupBlobFn func(t restic.BlobType, id restic.ID) []restic.PackedBlob, useBlobCache bool) (idx *RechunkerIndex, err error) {
	// collect blob usage info
	blobRemaining := map[restic.ID]int{}
	for _, file := range filesList {
		for _, blob := range file.IDs {
			blobRemaining[blob]++
		}
	}

	// build blob lookup info
	blobSize := map[restic.ID]uint{}
	blobToPack := map[restic.ID]restic.ID{}
	packToBlobs := map[restic.ID][]restic.Blob{}
	for blob := range blobRemaining {
		packs := lookupBlobFn(restic.DataBlob, blob)
		if len(packs) == 0 {
			return nil, fmt.Errorf("can't find blob from source repo: %v", blob)
		}
		pb := packs[0]

		blobSize[pb.Blob.ID] = pb.DataLength()
		blobToPack[pb.Blob.ID] = pb.PackID
		packToBlobs[pb.PackID] = append(packToBlobs[pb.PackID], pb.Blob)
	}

	idx = &RechunkerIndex{
		blobSize:      blobSize,
		blobToPack:    blobToPack,
		packToBlobs:   packToBlobs,
		blobRemaining: blobRemaining,
	}

	if !useBlobCache { // that's all you need if blob cache is disabled
		return idx, nil
	}

	// build blob trace info for small files
	// if blob cache is enabled, Rechunker tracks small files' remaining number of
	// blobs until all blobs are readily available in the cache (sfBlobRequires);
	// when the file has all its blobs ready, it is prioritized to be processed first.
	// this logic is handled by rc.priorityFilesHandler.
	sfBlobRequires := map[restic.ID]int{}
	sfBlobToFiles := map[restic.ID][]*ChunkedFile{}
	for _, file := range filesList {
		if file.Len() >= SMALL_FILE_THRESHOLD {
			continue
		}
		blobSet := restic.NewIDSet(file.IDs...)
		sfBlobRequires[file.hashval] = len(blobSet)
		for b := range blobSet {
			sfBlobToFiles[b] = append(sfBlobToFiles[b], file)
		}
	}

	idx.sfBlobRequires = sfBlobRequires
	idx.sfBlobToFiles = sfBlobToFiles

	return idx, nil
}

func startCache(ctx context.Context, wg *errgroup.Group, srcRepo restic.Repository, idx *RechunkerIndex, numDownloaders int, cacheSize int) (*BlobCache, *PriorityFilesHandler) {
	debug.Log("Initiating priorityFilesHandler")
	priorityFilesHandler := NewPriorityFilesHandler()

	debug.Log("initiating cache")
	cache := NewBlobCache(ctx, wg, cacheSize, numDownloaders, idx.blobToPack, idx.packToBlobs, srcRepo,
		func(blobIDs restic.IDs) {
			// onReady() implementation
			// when a new blob is ready, (small) files containing that blob has
			// their sfBlobRequires decreased by one. When all blobs for
			// the file is ready, it is pushed into priorityFilesHandler.

			var readyFiles []*ChunkedFile

			idx.sfBlobRequiresLock.Lock()
			for _, id := range blobIDs {
				filesToUpdate := idx.sfBlobToFiles[id]

				for _, file := range filesToUpdate {
					if idx.sfBlobRequires[file.hashval] > 0 {
						idx.sfBlobRequires[file.hashval]--
						if idx.sfBlobRequires[file.hashval] == 0 {
							readyFiles = append(readyFiles, file)
						}
					}
				}
			}
			idx.sfBlobRequiresLock.Unlock()

			_ = priorityFilesHandler.Push(readyFiles)

			// debugNote: trace blob load count
			debugNoteLock.Lock()
			for _, id := range blobIDs {
				debugNote["load:"+id.String()]++
			}
			debugNoteLock.Unlock()
		}, func(blobIDs restic.IDs) {
			// onEvict() implementation
			// when a blob is evicted, (small) files containing that blob has
			// their sfBlobRequires increased by one. However, ignore files once pushed
			// into priorityFilesHandler; as they are already sent to priorityTrack.

			idx.sfBlobRequiresLock.Lock()
			for _, id := range blobIDs {
				filesToUpdate := idx.sfBlobToFiles[id]
				for _, file := range filesToUpdate {
					// files with sPackRequires==0 has already gone to priorityFilesList, so don't track them
					if idx.sfBlobRequires[file.hashval] > 0 {
						idx.sfBlobRequires[file.hashval]++
					}
				}
			}
			idx.sfBlobRequiresLock.Unlock()
		},
	)

	return cache, priorityFilesHandler
}

func createFilesListDispatchChannel(ctx context.Context, wg *errgroup.Group, filesList []*ChunkedFile, visited func(id restic.ID) bool, onFinishCB func()) <-chan *ChunkedFile {
	ch := make(chan *ChunkedFile)
	wg.Go(func() error {
		for _, file := range filesList {
			// check if the file was visited by another dispatcher;
			// if it was, skip the file.
			if visited != nil && visited(file.hashval) {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- file:
			}
		}
		close(ch)

		if onFinishCB != nil {
			onFinishCB()
		}
		return nil
	})

	return ch
}

func createPriorityDispatchChannel(ctx context.Context, wg *errgroup.Group, h *PriorityFilesHandler, visited func(id restic.ID) bool) <-chan *ChunkedFile {
	ch := make(chan *ChunkedFile)
	wg.Go(func() error {
		defer close(ch)

		var priorityTrack []*ChunkedFile
		for {
			if len(priorityTrack) == 0 {
				// wait for priority files arrival or done signal
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-h.Done():
					return nil
				case <-h.Arrival():
					priorityTrack = h.Pop()
					continue
				}
			}

			file := priorityTrack[0]
			priorityTrack = priorityTrack[1:]

			// check if the file was handled by another dispatcher;
			// if it was, skip the file.
			if visited != nil && visited(file.hashval) {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-h.Done():
				return nil
			case ch <- file:
			}
		}
	})

	return ch
}

func startDispatcher(ctx context.Context, wg *errgroup.Group, filesList []*ChunkedFile, cache *BlobCache, priorityFilesHandler *PriorityFilesHandler) (chRegular, chPriority <-chan *ChunkedFile) {
	if cache != nil {
		// if cache is enabled

		// visited files set shared among dispatchers
		visitedFiles := restic.IDSet{}
		visitedFilesLock := sync.Mutex{}

		// goroutine for priority track (chPriority)
		debug.Log("Running dispatcher for priority channel")
		chPriority = createPriorityDispatchChannel(ctx, wg, priorityFilesHandler, func(id restic.ID) bool {
			visitedFilesLock.Lock()
			visited := visitedFiles.Has(id)
			if !visited {
				visitedFiles.Insert(id)
			}
			visitedFilesLock.Unlock()
			return visited
		})

		// goroutine for regular track (chRegular)
		debug.Log("Running dispatcher for regular channel")
		chRegular = createFilesListDispatchChannel(ctx, wg, filesList, func(id restic.ID) bool {
			visitedFilesLock.Lock()
			visited := visitedFiles.Has(id)
			if !visited {
				visitedFiles.Insert(id)
			}
			visitedFilesLock.Unlock()
			return visited
		}, func() {
			priorityFilesHandler.Close()
		})
	} else {
		// if cache is disabled: use regular track only

		chRegular = createFilesListDispatchChannel(ctx, wg, filesList, nil, nil)
	}

	return chRegular, chPriority
}

func (rc *Rechunker) runWorkers(ctx context.Context, wg *errgroup.Group, numWorkers int, getBlob getBlobFn, dstRepo restic.BlobSaver, chFirst <-chan *ChunkedFile, chSecond <-chan *ChunkedFile, onBlobLoad blobLoadCallbackFn, p *progress.Counter) {
	if chFirst == nil { // assertion
		panic("chFirst must not be nil")
	}

	bufferPool := make(chan []byte, 4*numWorkers)

	for range numWorkers {
		wg.Go(func() error {
			chnker := chunker.New(nil, rc.pol)

			for {
				// prioritize chFirst over chSecond
				file, ok, err := prioritySelect(ctx, chFirst, chSecond)
				if err != nil {
					return err
				}
				if !ok { // either of two channels is closed
					return nil
				}

				srcBlobs := file.IDs
				dstBlobs := restic.IDs{}

				// channels for pipeline
				chStreamer := make(chan fileStreamReader)
				chChunk := make(chan chunk)
				chFastForward := make(chan fastForward, 1)
				chDstBlobs := make(chan restic.IDs, 1)

				wgIn, ctxIn := errgroup.WithContext(ctx)

				// data preparation for ChunkDict
				useChunkDict := len(srcBlobs) != 0 && len(srcBlobs) >= LARGE_FILE_THRESHOLD
				if useChunkDict {
					// prepare chunkDict context, and check for prefix match
					info, ff, prefix := prepareChunkDict(ctxIn, wgIn, srcBlobs, rc.chunkDict, rc.idx)
					if ff != nil {
						dstBlobs = prefix
						chFastForward <- *ff
					}

					// start pipeline
					startFileStreamerWithFastForward(ctxIn, wgIn, srcBlobs, chStreamer, getBlob, onBlobLoad, bufferPool, chFastForward)
					startChunker(ctxIn, wgIn, chnker, rc.pol, chStreamer, chChunk, bufferPool)
					startFileBlobSaverWithFastForward(ctxIn, wgIn, chChunk, chDstBlobs, dstRepo, bufferPool, chFastForward, info)
				} else {
					// not using chunkDict; simply stream-process from head to EOF

					// start pipeline
					startFileStreamer(ctxIn, wgIn, srcBlobs, chStreamer, getBlob, onBlobLoad, bufferPool)
					startChunker(ctxIn, wgIn, chnker, rc.pol, chStreamer, chChunk, bufferPool)
					startFileBlobSaver(ctxIn, wgIn, chChunk, chDstBlobs, dstRepo, bufferPool)
				}

				err = wgIn.Wait()
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
			}
		})
	}
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
