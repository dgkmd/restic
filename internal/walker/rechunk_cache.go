package walker

import (
	"sync"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/restic/restic/internal/restic"
)

type link interface{}
type terminalLink struct {
	dstBlob restic.ID
	offset uint32
}
type continuedLink map[restic.ID]link
type linkIndex map[uint32]link

type RechunkChainDict struct {
	d map[restic.ID]linkIndex
	nullID restic.ID
}

func NewRechunkChainDict() *RechunkChainDict {
	return &RechunkChainDict{
		d: make(map[restic.ID]linkIndex),
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
	panic("not implemented")
}


type PackLRU = *lru.Cache[restic.ID, []restic.ID]

type packedBlobData struct {
	data []byte
	packID restic.ID
}

type RechunkBlobCache struct {
	pcklru PackLRU
	blobs map[restic.ID]packedBlobData
	blobsLock sync.RWMutex
}

func NewRechunkBlobCache() *RechunkBlobCache {
	rbc := &RechunkBlobCache{
		blobs: make(map[restic.ID]packedBlobData),
	}
	lru, err := lru.NewWithEvict[restic.ID, []restic.ID](1000, rbc.onEvict)
	if err != nil {
		panic(err)
	}
	rbc.pcklru = lru

	return rbc
}

func (rbc *RechunkBlobCache) onEvict(k restic.ID, v []restic.ID) {
	rbc.blobsLock.Lock()
	for _, blob := range v {
		delete(rbc.blobs, blob)
	}
	rbc.blobsLock.Unlock()
}

func (rbc *RechunkBlobCache) Get(id restic.ID) []byte {
	rbc.blobsLock.RLock()
	blob, ok := rbc.blobs[id]
	rbc.blobsLock.RUnlock()
	if ok {
		_, _ = rbc.pcklru.Get(blob.packID) // update recency
		return blob.data
	} else {
		// 1. download the pack in which the blob resides
		// 2. store to the cache all needed blobs in the pack
		// 3. register the pack to packLRU
		// 4. update 'ready' files set which is a subset of all small files
		// 5. and finally, return blob.data
	}

	return nil
}