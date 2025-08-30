package walker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
	"golang.org/x/sync/errgroup"
)

type hashType = [32]byte
type fileType struct {
	restic.IDs
	blobsHash hashType
}

type Rechunker struct {
	srcRepo    restic.Repository
	dstRepo    restic.Repository
	pol        chunker.Pol
	blobLoader restic.BlobLoader
	blobSaver  restic.BlobSaver
	cache      *RechunkBlobCache

	// immutable once built (by design)
	filesList     []restic.IDs
	blobToPackMap map[restic.ID]restic.ID

	// RW by dedicated worker
	blobRequires    map[restic.ID]int
	sPackRequires   map[hashType]int
	sPackToFilesMap map[restic.ID][]fileType

	// concurrently accessed across workers
	priorityFilesList     []restic.IDs
	packToBlobsMap        map[restic.ID][]restic.Blob
	rechunkMap            map[hashType]restic.IDs
	priorityFilesListLock sync.Mutex
	packToBlobsMapLock    sync.Mutex
	rechunkMapLock        sync.Mutex

	// used in RewriteTree
	rewriteTreeMap map[restic.ID]restic.ID
}

func NewRechunker(srcRepo restic.Repository, dstRepo restic.Repository) *Rechunker {
	return &Rechunker{
		srcRepo:    srcRepo,
		dstRepo:    dstRepo,
		pol:        dstRepo.Config().ChunkerPolynomial,
		blobLoader: srcRepo,
		blobSaver:  dstRepo,

		filesList:         []restic.IDs{},
		blobToPackMap:     map[restic.ID]restic.ID{},
		blobRequires:      map[restic.ID]int{},
		sPackRequires:     map[hashType]int{},
		sPackToFilesMap:   map[restic.ID][]fileType{},
		priorityFilesList: []restic.IDs{},
		packToBlobsMap:    map[restic.ID][]restic.Blob{},
		rechunkMap:        map[hashType]restic.IDs{},
		rewriteTreeMap:    map[restic.ID]restic.ID{},
	}
}

const SMALL_FILE_THRESHOLD = 25

func (rc *Rechunker) Plan(ctx context.Context, roots []restic.ID) error {
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

		rc.packToBlobsMap[pb.PackID] = append(rc.packToBlobsMap[pb.PackID], pb.Blob)
		rc.blobToPackMap[pb.Blob.ID] = pb.PackID
	}

	// build pack<->file metadata for small files
	for _, blobs := range rc.filesList {
		if len(blobs) > SMALL_FILE_THRESHOLD {
			continue
		}
		hashval := hashOfIDs(blobs)
		packSet := restic.IDSet{}
		for _, blob := range blobs {
			pack := rc.blobToPackMap[blob]
			packSet[pack] = struct{}{}
		}
		rc.sPackRequires[hashval] = len(packSet)
		for p := range packSet {
			rc.sPackToFilesMap[p] = append(rc.sPackToFilesMap[p], fileType{blobs, hashval})
		}
	}

	return nil
}

func (rc *Rechunker) RechunkData(ctx context.Context, p *progress.Counter) error {
	numWorkers := max(runtime.GOMAXPROCS(0)-1, 1)
	bufferPool := make(chan []byte, 4*numWorkers)
	wgMgr, wgMgrCtx := errgroup.WithContext(ctx)
	wgWkr, wgWkrCtx := errgroup.WithContext(ctx)

	var blobGet func(blobID restic.ID, buf []byte) ([]byte, error)

	// blob cache
	rc.cache = NewRechunkBlobCache(wgMgrCtx, wgMgr, rc.blobToPackMap, func(packID restic.ID) (BlobData, error) {
		// downloadFn implementation
		blobData := BlobData{}
		rc.packToBlobsMapLock.Lock()
		blobs := rc.packToBlobsMap[packID]
		rc.packToBlobsMapLock.Unlock()
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
		files := rc.sPackToFilesMap[packID]
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
		files := rc.sPackToFilesMap[packID]
		for _, file := range files {
			// files with sPackRequires==0 has already gone to priorityFilesList, so ignore them
			if rc.sPackRequires[file.blobsHash] > 0 {
				rc.sPackRequires[file.blobsHash]++
			}
		}
	})
	// implement blobGet using blob cache
	blobGet = func(blobID restic.ID, buf []byte) ([]byte, error) {
		blob, ch := rc.cache.Get(wgWkrCtx, wgWkr, blobID, buf)
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
		priorityList := []restic.IDs{}

		for {
			if len(priorityList) == 0 {
				rc.priorityFilesListLock.Lock()
				if len(rc.priorityFilesList) > 0 {
					priorityList = rc.priorityFilesList
					rc.priorityFilesList = []restic.IDs{}
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

	// blob tracer
	chBlobLoaded := make(chan restic.ID, numWorkers)
	wgMgr.Go(func() error {
		for {
			var id restic.ID
			var ok bool
			select {
			case <-wgMgrCtx.Done():
				return wgMgrCtx.Err()
			case id, ok = <-chBlobLoaded:
				if !ok { // job complete
					return nil
				}
			}
			rc.blobRequires[id]--
			if rc.blobRequires[id] == 0 {
				pack := rc.blobToPackMap[id]
				rc.packToBlobsMapLock.Lock()
				rc.packToBlobsMap[pack] = slices.DeleteFunc(rc.packToBlobsMap[pack], func(b restic.Blob) bool { return b.ID == id })
				rc.packToBlobsMapLock.Unlock()
			}
		}
	})

	// rechunk workers
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
				wg, wgCtx := errgroup.WithContext(wgWkrCtx)

				// run loader/iopipe/chunker/saver Goroutines per each file
				// loader: load original chunks one by one from source repo
				chFront := make(chan []byte)
				wg.Go(func() error {
					for _, blobID := range srcBlobs {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							debug.Log("make buffer (1)")
							buf = make([]byte, chunker.MaxSize)
						}

						blob, err := blobGet(blobID, buf)
						if err != nil {
							return err
						}
						chBlobLoaded <- blobID

						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case chFront <- blob:
						}
					}
					close(chFront)
					return nil
				})

				// iopipe: convert chunks into io.Reader stream
				r, w := io.Pipe()
				wg.Go(func() error {
					for {
						var buf []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							w.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case buf, ok = <-chFront:
							if !ok { // EOF
								w.Close()
								return nil
							}
						}

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
				chnker.Reset(r, rc.pol)
				chBack := make(chan []byte)
				wg.Go(func() error {
					for {
						var buf []byte
						select {
						case buf = <-bufferPool:
						default:
							debug.Log("make buffer (2)")
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
						if err != nil {
							r.CloseWithError(err)
							return err
						}

						select {
						case <-wgCtx.Done():
							r.CloseWithError(wgCtx.Err())
							return wgCtx.Err()
						case chBack <- chunk.Data:
						}
					}
				})

				// saver: save rechunked blobs into destination repo
				wg.Go(func() error {
					for {
						var blobData []byte
						var ok bool
						select {
						case <-wgCtx.Done():
							return wgCtx.Err()
						case blobData, ok = <-chBack:
							if !ok { // EOF
								return nil
							}
						}

						blobID, _, _, err := rc.dstRepo.SaveBlob(ctx, restic.DataBlob, blobData, restic.ID{}, false)
						if err != nil {
							return err
						}

						select {
						case bufferPool <- blobData:
						default:
						}
						dstBlobs = append(dstBlobs, blobID)
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
	rc.cache.Close()
	close(chBlobLoaded)
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
