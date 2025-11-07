package rechunker

import (
	"context"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

const overhead = len(restic.ID{}) + 64

type BlobsMap = map[restic.ID][]byte
type PackDownloadQueue struct {
	waiter      chan struct{}
	wantedBlobs restic.IDSet
	allWanted   bool
}

type BlobCache struct {
	mu sync.RWMutex
	c  *simplelru.LRU[restic.ID, []byte]

	free, size int

	q          map[restic.ID]PackDownloadQueue // pack queue; added by Get(), deleted by downloaders
	inProgress map[restic.ID]chan struct{}     // blob ready event; written by downloaders, read by Get()
	downloadCh chan restic.ID                  // download request of pack ID
	evictCh    chan restic.IDs                 // evict request of blob ID
	ignores    restic.IDSet                    // set of no longer needed blobs

	blobToPack  map[restic.ID]restic.ID // readonly: map from blob ID to pack ID the blob resides in
	packToBlobs map[restic.ID][]restic.Blob

	done chan struct{}
}

func NewBlobCache(ctx context.Context, wg *errgroup.Group, size int, numDownloaders int,
	blobToPack map[restic.ID]restic.ID, packToBlobs map[restic.ID][]restic.Blob, repo PackedBlobLoader, onReady func(blobIDs restic.IDs),
	onEvict func(blobIDs restic.IDs)) *BlobCache {
	if size < 32*(1<<20) {
		panic("Blob cache size should be at least 32 MiB!!")
	}
	c := &BlobCache{
		size:        size,
		free:        size,
		downloadCh:  make(chan restic.ID),
		evictCh:     make(chan restic.IDs),
		q:           map[restic.ID]PackDownloadQueue{},
		inProgress:  map[restic.ID]chan struct{}{},
		ignores:     restic.IDSet{},
		blobToPack:  blobToPack,
		packToBlobs: packToBlobs,
		done:        make(chan struct{}),
	}
	lru, err := simplelru.NewLRU(size, func(k restic.ID, v []byte) {
		c.free += cap(v) + overhead
		onEvict(restic.IDs{k})
	})
	if err != nil {
		panic(err)
	}
	c.c = lru

	download := func(packID restic.ID, blobs []restic.Blob) (blobsMap BlobsMap, err error) {
		blobData := BlobsMap{}
		err = repo.LoadBlobsFromPack(ctx, packID, blobs,
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
	}

	// blob downloaders
	for range numDownloaders {
		wg.Go(func() error {
			for {
				var packID restic.ID
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-c.done:
					return nil
				case packID = <-c.downloadCh:
				}

				c.mu.Lock()
				q := c.q[packID]
				delete(c.q, packID)
				var filteredBlobs []restic.Blob
				for _, blob := range c.packToBlobs[packID] {
					wanted := q.allWanted || q.wantedBlobs.Has(blob.ID)
					ignored := c.ignores.Has(blob.ID)
					_, inProgress := c.inProgress[blob.ID]
					ready := c.c.Contains(blob.ID)
					if wanted && !ignored && !inProgress && !ready {
						filteredBlobs = append(filteredBlobs, blob)
					}
				}
				for _, blob := range filteredBlobs {
					c.inProgress[blob.ID] = make(chan struct{})
				}
				close(q.waiter)
				c.mu.Unlock()

				if len(filteredBlobs) == 0 {
					continue
				}

				blobsMap, err := download(packID, filteredBlobs)
				if err != nil {
					return err
				}

				var blobIDs restic.IDs
				c.mu.Lock()
				for id, data := range blobsMap {
					size := cap(data) + overhead
					for size > c.free {
						c.c.RemoveOldest()
					}
					c.c.Add(id, data)
					c.free -= size
					close(c.inProgress[id])
					delete(c.inProgress, id)
					blobIDs = append(blobIDs, id)
				}
				c.mu.Unlock()

				if onReady != nil {
					onReady(blobIDs)
				}
			}
		})
	}

	// blob ignorer
	wg.Go(func() error {
		for {
			var ids restic.IDs
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.done:
				return nil
			case ids = <-c.evictCh:
			}

			c.mu.Lock()
			for _, id := range ids {
				c.ignores.Insert(id)
				c.c.Remove(id)
			}
			c.mu.Unlock()
		}
	})

	return c
}

func (c *BlobCache) Get(ctx context.Context, wg *errgroup.Group, id restic.ID, buf []byte, prefetch restic.IDs) ([]byte, chan []byte) {
	c.mu.Lock()
	blob, ok := c.c.Get(id) // try to retrieve blob, with recency update
	c.mu.Unlock()
	if ok { // case 1: when blob exists in cache: return that blob immediately
		if cap(buf) < len(blob) {
			debug.Log("buffer has smaller capacity than chunk size. Something might be wrong!")
			buf = make([]byte, len(blob))
		} else {
			buf = buf[:len(blob)]
		}
		copy(buf, blob)
		return buf, nil
	}

	// case 2: when blob does not exist in cache: return chOut (where downloaded blob will be delievered)
	chOut := make(chan []byte, 1)
	wg.Go(func() error {
		for {
			c.mu.RLock()
			blob, ready := c.c.Peek(id)
			finish, inProgress := c.inProgress[id]
			c.mu.RUnlock()

			if ready { // case 2A: blob is now ready in the cache
				if cap(buf) < len(blob) {
					debug.Log("buffer has smaller capacity than chunk size. Something might be wrong!")
					buf = make([]byte, len(blob))
				} else {
					buf = buf[:len(blob)]
				}
				copy(buf, blob)
				chOut <- buf
				return nil
			}
			if inProgress { // case 2B: blob is being downloaded now
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-finish: // wait until download complete
					continue
				}
			}

			// case 2C: blob is neither in the cache nor being downloaded

			// construct validBlobs set for prefetch
			packID := c.blobToPack[id]
			validBlobs := restic.NewIDSet(id)
			var allWanted bool
			if prefetch == nil {
				allWanted = true
			} else {
				for _, b := range prefetch {
					if pid := c.blobToPack[b]; packID == pid {
						validBlobs.Insert(b)
					}
				}
			}

			c.mu.Lock()
			q, ok := c.q[packID]
			if ok { // case 2C-i: pack is in the queue: just add validBlobs to wanted list
				if allWanted {
					q.allWanted = true
				} else {
					q.wantedBlobs.Merge(validBlobs)
				}
			} else { // case 2C-ii: pack is not in the queue: create new one
				q = PackDownloadQueue{
					waiter:      make(chan struct{}),
					wantedBlobs: validBlobs,
					allWanted:   allWanted,
				}
				c.q[packID] = q
			}
			c.mu.Unlock()

			if !ok { // if you are one who created the queue, send packID to inform the downloader
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c.downloadCh <- packID:
				}
			}

			// wait until it starts downloading
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-q.waiter:
			}
		}
	})
	return nil, chOut
}

func (c *BlobCache) Ignore(ctx context.Context, blobs restic.IDs) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.evictCh <- blobs:
		return nil
	}
}

func (c *BlobCache) Close() {
	if c == nil {
		return
	}

	select {
	case <-c.done:
	default:
		close(c.done)
	}
}
