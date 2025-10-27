package rechunker

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

const LRU_SIZE = 200

type PackLRU = *lru.Cache[restic.ID, []restic.ID]

type packedBlobData struct {
	data   []byte
	packID restic.ID
}
type BlobsMap = map[restic.ID][]byte

type PackCache struct {
	pcklru         PackLRU
	packDownloadCh chan restic.ID
	blobToPack     map[restic.ID]restic.ID

	blobs          map[restic.ID]packedBlobData
	packWaiter     map[restic.ID]chan struct{}
	blobsLock      sync.RWMutex
	packWaiterLock sync.Mutex

	closed bool
}

func NewPackCache(ctx context.Context, wg *errgroup.Group, blobToPack map[restic.ID]restic.ID, numDownloaders int,
	downloadFn func(packID restic.ID) (BlobsMap, error), onPackReady func(packID restic.ID), onPackEvict func(packID restic.ID)) *PackCache {
	pc := &PackCache{
		packDownloadCh: make(chan restic.ID),
		blobToPack:     blobToPack,
		blobs:          map[restic.ID]packedBlobData{},
		packWaiter:     map[restic.ID]chan struct{}{},
	}
	lru, err := lru.NewWithEvict(LRU_SIZE, func(k restic.ID, v []restic.ID) {
		pc.packWaiterLock.Lock()
		delete(pc.packWaiter, k)
		pc.packWaiterLock.Unlock()
		pc.blobsLock.Lock()
		for _, blob := range v {
			delete(pc.blobs, blob)
		}
		pc.blobsLock.Unlock()
		if onPackEvict != nil {
			onPackEvict(k)
		}
	})
	if err != nil {
		panic(err)
	}
	pc.pcklru = lru

	// start pack downloader
	for range numDownloaders {
		wg.Go(func() error {
			for {
				var packID restic.ID
				var ok bool
				select {
				case <-ctx.Done():
					return ctx.Err()
				case packID, ok = <-pc.packDownloadCh:
					if !ok { // job complete
						return nil
					}
				}

				if pc.pcklru.Contains(packID) {
					// pack already downloaded by the previous request
					continue
				}
				blobsMap, err := downloadFn(packID)
				if err != nil {
					return err
				}
				blobIDs := make([]restic.ID, 0, len(blobsMap))
				for id := range blobsMap {
					blobIDs = append(blobIDs, id)
				}
				pc.blobsLock.Lock()
				for id, data := range blobsMap {
					pc.blobs[id] = packedBlobData{
						data:   data,
						packID: packID,
					}
				}
				pc.blobsLock.Unlock()
				_ = pc.pcklru.Add(packID, blobIDs)
				if onPackReady != nil {
					onPackReady(packID)
				}
				pc.packWaiterLock.Lock()
				close(pc.packWaiter[packID])
				pc.packWaiterLock.Unlock()
			}
		})
	}

	return pc
}

func (pc *PackCache) Get(ctx context.Context, wg *errgroup.Group, id restic.ID, buf []byte) ([]byte, chan []byte) {
	pc.blobsLock.RLock()
	blob, ok := pc.blobs[id]
	pc.blobsLock.RUnlock()
	if ok { // when blob exists in cache: return that blob
		_, _ = pc.pcklru.Get(blob.packID) // update recency
		if cap(buf) < len(blob.data) {
			debug.Log("buffer has smaller capacity than chunk size. Something might be wrong!")
			buf = make([]byte, len(blob.data))
		}
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		return buf, nil
	}

	// when blob does not exist in cache: return async ch and send corresponding packID to downloader
	ch := make(chan []byte, 1) // where the downloaded blob will be delivered
	wg.Go(func() error {
		packID := pc.blobToPack[id]

		pc.packWaiterLock.Lock()
		chWaiter, ok := pc.packWaiter[packID]
		if !ok {
			chWaiter = make(chan struct{})
			pc.packWaiter[packID] = chWaiter
		}
		pc.packWaiterLock.Unlock()
		if !ok {
			// debug trace
			debug.Log("Cache miss: blob %v. Requesting download: pack %v...", id.Str(), packID.Str())

			select {
			case <-ctx.Done():
				return ctx.Err()
			case pc.packDownloadCh <- packID:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-chWaiter:
		}
		pc.blobsLock.RLock()
		blob, ok = pc.blobs[id]
		pc.blobsLock.RUnlock()
		if !ok {
			return fmt.Errorf("blob entry missing right after pack download. Please report this error at https://github.com/restic/restic/issues/")
		}
		if cap(buf) < len(blob.data) {
			debug.Log("buffer has smaller capacity than chunk size. Something might be wrong!")
			buf = make([]byte, len(blob.data))
		}
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		ch <- buf
		return nil
	})
	return nil, ch
}

func (pc *PackCache) Close() {
	if !pc.closed {
		close(pc.packDownloadCh)
		pc.closed = true
	}
}
