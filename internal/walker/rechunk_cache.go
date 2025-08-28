package walker

import (
	"context"
	"fmt"
	"sync"
	// "time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

type link interface{}
type terminalLink struct {
	dstBlob restic.ID
	offset  uint32
}
type continuedLink map[restic.ID]link
type linkIndex map[uint32]link

type RechunkChainDict struct {
	d      map[restic.ID]linkIndex
	nullID restic.ID
}

func NewRechunkChainDict() *RechunkChainDict {
	return &RechunkChainDict{
		d: map[restic.ID]linkIndex{},
	}
}

func (rcc *RechunkChainDict) Get(srcBlobs restic.IDs, offset uint32) (dstBlobs restic.IDs, numFinishedBlobs int, newOffset uint32) {
	if len(srcBlobs) == 0 {
		return
	}
	c, ok := rcc.d[srcBlobs[0]][offset]
	if !ok {
		return
	}

	currentConsumedBlobs := 0
	for {
		switch lnk := c.(type) {
		case terminalLink:
			if dstBlobs == nil {
				dstBlobs = restic.IDs{lnk.dstBlob}
			} else {
				dstBlobs = append(dstBlobs, lnk.dstBlob)
			}
			newOffset = lnk.offset
			numFinishedBlobs += currentConsumedBlobs
			currentConsumedBlobs = 0

			if len(srcBlobs) == 0 {
				return
			}
			c, ok = rcc.d[srcBlobs[0]][newOffset]
			if !ok {
				return
			}
		case continuedLink:
			currentConsumedBlobs++
			srcBlobs = srcBlobs[1:]

			if len(srcBlobs) == 0 {
				c, ok = lnk[rcc.nullID]
				if !ok {
					return
				}
			} else {
				c, ok = lnk[srcBlobs[0]]
				if !ok {
					return
				}
			}
		default:
			panic("wrong type")
		}
	}
}

func (rcc *RechunkChainDict) Set(srcBlobs restic.IDs, startOffset, endOffset uint32, dstBlob restic.ID) error {
	panic("not implemented yet")
}

//////////

// var dbg = map[string]float64{}
// var dbgLock = sync.Mutex{}

// func dbgTime(name string, since time.Time) {
// 	dbgLock.Lock()
// 	dbg[name] += time.Since(since).Seconds()
// 	debug.Log("%v: %v", name, dbg[name])
// 	dbgLock.Unlock()
// }

const LRU_SIZE = 100

type PackLRU = *lru.Cache[restic.ID, []restic.ID]

type packBlobData struct {
	data   []byte
	packID restic.ID
}
type BlobData = map[restic.ID][]byte

type RechunkBlobCache struct {
	pcklru         PackLRU
	blobs          map[restic.ID]packBlobData
	blobsLock      sync.RWMutex
	packDownloadCh chan restic.ID
	packWaiter     map[restic.ID]chan struct{}
	packWaiterLock sync.Mutex
	blobToPackMap  map[restic.ID]restic.ID
}

func NewRechunkBlobCache(ctx context.Context, wg *errgroup.Group, blobToPackMap map[restic.ID]restic.ID,
	downloadFn func(packID restic.ID) (BlobData, error), onReady func(packID restic.ID), onEvict func(packID restic.ID)) *RechunkBlobCache {
	rbc := &RechunkBlobCache{
		blobs:          map[restic.ID]packBlobData{},
		packDownloadCh: make(chan restic.ID),
		packWaiter:     map[restic.ID]chan struct{}{},
		blobToPackMap:  blobToPackMap,
	}
	lru, err := lru.NewWithEvict(LRU_SIZE, func(k restic.ID, v []restic.ID) {
		// dbgStart := time.Now()
		rbc.blobsLock.Lock()
		for _, blob := range v {
			delete(rbc.blobs, blob)
		}
		rbc.packWaiterLock.Lock()
		delete(rbc.packWaiter, k)
		rbc.packWaiterLock.Unlock()
		rbc.blobsLock.Unlock()
		// dbgTime("evict", dbgStart)
		if onEvict != nil {
			onEvict(k)
		}
	})
	if err != nil {
		panic(err)
	}
	rbc.pcklru = lru

	wg.Go(func() error {
		for {
			var packID restic.ID
			var ok bool
			select {
			case <-ctx.Done():
				return ctx.Err()
			case packID, ok = <-rbc.packDownloadCh:
				if !ok { // job complete
					return nil
				}
			}

			if rbc.pcklru.Contains(packID) {
				// pack already downloaded by the previous request
				continue
			}
			// dbgStart := time.Now()
			blobData, err := downloadFn(packID)
			// dbgTime("download", dbgStart)
			if err != nil {
				return err
			}
			blobIDs := make([]restic.ID, 0, len(blobData))
			for id := range blobData {
				blobIDs = append(blobIDs, id)
			}
			rbc.blobsLock.Lock()
			for id, data := range blobData {
				rbc.blobs[id] = packBlobData{
					data:   data,
					packID: packID,
				}
			}
			rbc.blobsLock.Unlock()
			_ = rbc.pcklru.Add(packID, blobIDs)
			if onReady != nil {
				onReady(packID)
			}
			rbc.packWaiterLock.Lock()
			close(rbc.packWaiter[packID])
			rbc.packWaiterLock.Unlock()
		}
	})

	return rbc
}

func (rbc *RechunkBlobCache) Get(ctx context.Context, wg *errgroup.Group, id restic.ID, buf []byte) ([]byte, chan []byte) {
	rbc.blobsLock.RLock()
	blob, ok := rbc.blobs[id]
	rbc.blobsLock.RUnlock()
	if ok {
		// debug.Log("cache hit")
		_, _ = rbc.pcklru.Get(blob.packID) // update recency
		if cap(buf) < len(blob.data) {
			debug.Log("received buffer has size smaller than chunk. It's likely that something is wrong!")
			buf = make([]byte, len(blob.data))
		}
		// dbgStart := time.Now()
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		// dbgTime("blobcopy", dbgStart)
		return buf, nil
	}

	// when blob does not exist in cache
	chBlobData := make(chan []byte, 1)
	wg.Go(func() error {
		packID := rbc.blobToPackMap[id]
		rbc.packWaiterLock.Lock()
		ch, ok := rbc.packWaiter[packID]
		if !ok {
			ch = make(chan struct{})
			rbc.packWaiter[packID] = ch
		}
		rbc.packWaiterLock.Unlock()
		if !ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case rbc.packDownloadCh <- packID:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
		rbc.blobsLock.RLock()
		blob, ok = rbc.blobs[id]
		rbc.blobsLock.RUnlock()
		if !ok {
			return fmt.Errorf("blob entry missing right after pack download. Please report this error at https://github.com/restic/restic/issues/")
		}
		// dbgStart := time.Now()
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		// dbgTime("blobcopy", dbgStart)
		chBlobData <- buf
		return nil
	})

	return nil, chBlobData
}

func (rbc *RechunkBlobCache) Stop() {
	close(rbc.packDownloadCh)
}
