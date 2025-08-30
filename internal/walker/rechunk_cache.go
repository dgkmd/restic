package walker

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

type link interface{} // union of {terminalLink, continuedLink}
type terminalLink struct {
	dstBlob restic.ID
	offset  uint
}
type continuedLink map[restic.ID]link
type linkIndex map[uint]link
type chainDict map[restic.ID]linkIndex

type RechunkChainDict struct {
	dict   chainDict
	lock   sync.RWMutex
	nullID restic.ID
}

func NewRechunkChainDict() *RechunkChainDict {
	return &RechunkChainDict{
		dict: chainDict{},
	}
}

func (rcd *RechunkChainDict) Get(srcBlobs restic.IDs, offset uint) (dstBlobs restic.IDs, numFinishedBlobs int, newOffset uint) {
	if len(srcBlobs) == 0 { // nothing to return
		return
	}

	rcd.lock.RLock()
	defer rcd.lock.RUnlock()
	lnk, ok := rcd.dict[srcBlobs[0]][offset]
	if !ok { // dict entry not found
		return
	}

	currentConsumedBlobs := 0
	for {
		switch v := lnk.(type) {
		case terminalLink:
			if dstBlobs == nil {
				dstBlobs = restic.IDs{}
			}
			dstBlobs = append(dstBlobs, v.dstBlob)

			newOffset = v.offset
			numFinishedBlobs += currentConsumedBlobs
			currentConsumedBlobs = 0

			if len(srcBlobs) == 0 { // EOF
				return
			}
			lnk, ok = rcd.dict[srcBlobs[0]][newOffset]
			if !ok {
				return
			}
		case continuedLink:
			currentConsumedBlobs++
			srcBlobs = srcBlobs[1:]

			if len(srcBlobs) == 0 { // reached EOF
				lnk, ok = v[rcd.nullID]
				if !ok {
					return
				}
			} else { // go on to next blob
				lnk, ok = v[srcBlobs[0]]
				if !ok {
					return
				}
			}
		default:
			panic("wrong type")
		}
	}
}

func (rcd *RechunkChainDict) Add(srcBlobs restic.IDs, startOffset, endOffset uint, dstBlob restic.ID) error {
	if len(srcBlobs) == 0 {
		return fmt.Errorf("empty srcBlobs")
	}
	if len(srcBlobs) == 1 && startOffset > endOffset {
		return fmt.Errorf("wrong value. len(srcBlob)==1 and startOffset>endOffset")
	}

	numContinuedLink := len(srcBlobs)
	if endOffset != 0 {
		numContinuedLink--
	}

	rcd.lock.Lock()
	defer rcd.lock.Unlock()
	idx, ok := rcd.dict[srcBlobs[0]]
	if !ok {
		rcd.dict[srcBlobs[0]] = idx
	}
	lnk, ok := idx[startOffset]
	shouldBeTerminalLink := (numContinuedLink == 0)
	if ok { // type assertion
		if shouldBeTerminalLink {
			_ = lnk.(terminalLink)
			return nil
		} else {
			_ = lnk.(continuedLink)
		}
	} else { // index does not exist
		if shouldBeTerminalLink {
			idx[startOffset] = terminalLink{
				dstBlob: dstBlob,
				offset:  endOffset,
			}
			return nil
		} else {
			idx[startOffset] = continuedLink{}
			lnk = idx[startOffset]
		}
	}
	srcBlobs = srcBlobs[1:]
	for range numContinuedLink - 1 {
		v := lnk.(continuedLink)
		lnk, ok = v[srcBlobs[0]]
		if !ok {
			v[srcBlobs[0]] = continuedLink{}
			lnk = v[srcBlobs[0]]
		}
		srcBlobs = srcBlobs[1:]
	}
	v := lnk.(continuedLink)
	var blob restic.ID
	if len(srcBlobs) == 0 { // should branch to here if endOffset == 0
		blob = rcd.nullID
	} else if len(srcBlobs) == 1 { // should branch to here if endOffset != 0
		blob = srcBlobs[0]
	} else {
		panic("faulty logic")
	}
	lnk, ok = v[blob]
	if ok {
		_ = lnk.(terminalLink)
	} else {
		v[blob] = terminalLink{
			dstBlob: dstBlob,
			offset:  endOffset,
		}
	}

	return nil
}

//////////

const LRU_SIZE = 100

type PackLRU = *lru.Cache[restic.ID, []restic.ID]

type packBlobData struct {
	data   []byte
	packID restic.ID
}
type BlobData = map[restic.ID][]byte

type RechunkBlobCache struct {
	pcklru         PackLRU
	packDownloadCh chan restic.ID
	blobLookup     map[restic.ID]blobInfo

	blobs          map[restic.ID]packBlobData
	packWaiter     map[restic.ID]chan struct{}
	blobsLock      sync.RWMutex
	packWaiterLock sync.Mutex
}

func NewRechunkBlobCache(ctx context.Context, wg *errgroup.Group, blobLookup map[restic.ID]blobInfo,
	downloadFn func(packID restic.ID) (BlobData, error), onPackReady func(packID restic.ID), onPackEvict func(packID restic.ID)) *RechunkBlobCache {
	rbc := &RechunkBlobCache{
		packDownloadCh: make(chan restic.ID),
		blobLookup:     blobLookup,
		blobs:          map[restic.ID]packBlobData{},
		packWaiter:     map[restic.ID]chan struct{}{},
	}
	lru, err := lru.NewWithEvict(LRU_SIZE, func(k restic.ID, v []restic.ID) {
		rbc.blobsLock.Lock()
		for _, blob := range v {
			delete(rbc.blobs, blob)
		}
		rbc.packWaiterLock.Lock()
		delete(rbc.packWaiter, k)
		rbc.packWaiterLock.Unlock()
		rbc.blobsLock.Unlock()
		if onPackEvict != nil {
			onPackEvict(k)
		}
	})
	if err != nil {
		panic(err)
	}
	rbc.pcklru = lru

	// start pack downloader
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
			blobData, err := downloadFn(packID)
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
			if onPackReady != nil {
				onPackReady(packID)
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
	if ok { // when blob exists in cache: return that blob
		_, _ = rbc.pcklru.Get(blob.packID) // update recency
		if cap(buf) < len(blob.data) {
			debug.Log("received buffer has size smaller than chunk. It's likely that something is wrong!")
			buf = make([]byte, len(blob.data))
		}
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		return buf, nil
	}

	// when blob does not exist in cache: return async ch and queue pack download
	ch := make(chan []byte, 1)
	wg.Go(func() error {
		packID := rbc.blobLookup[id].packID
		rbc.packWaiterLock.Lock()
		chWaiter, ok := rbc.packWaiter[packID]
		if !ok {
			chWaiter = make(chan struct{})
			rbc.packWaiter[packID] = chWaiter
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
		case <-chWaiter:
		}
		rbc.blobsLock.RLock()
		blob, ok = rbc.blobs[id]
		rbc.blobsLock.RUnlock()
		if !ok {
			return fmt.Errorf("blob entry missing right after pack download. Please report this error at https://github.com/restic/restic/issues/")
		}
		buf = buf[:len(blob.data)]
		copy(buf, blob.data)
		ch <- buf
		return nil
	})
	return nil, ch
}

func (rbc *RechunkBlobCache) Close() {
	close(rbc.packDownloadCh)
}
