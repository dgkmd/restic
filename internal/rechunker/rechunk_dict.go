package rechunker

import (
	"fmt"
	"sync"

	"github.com/restic/restic/internal/restic"
)

type link interface{} // union of {terminalLink, connectingLink}
type terminalLink struct {
	dstBlob restic.ID
	offset  uint
}
type connectingLink map[restic.ID]link
type linkIndex map[uint]link
type chunkDict map[restic.ID]linkIndex

type ChunkDict struct {
	dict chunkDict
	lock sync.RWMutex
}

func NewChunkDict() *ChunkDict {
	return &ChunkDict{
		dict: chunkDict{},
	}
}

func (cd *ChunkDict) Match(srcBlobs restic.IDs, startOffset uint) (dstBlobs restic.IDs, numFinishedBlobs int, newOffset uint) {
	if len(srcBlobs) == 0 { // nothing to return
		return
	}

	cd.lock.RLock()
	defer cd.lock.RUnlock()

	lnk, ok := cd.dict[srcBlobs[0]][startOffset]
	if !ok { // dict entry not found
		return
	}

	currentConsumedBlobs := 0
	for {
		switch v := lnk.(type) {
		case terminalLink:
			dstBlobs = append(dstBlobs, v.dstBlob)
			newOffset = v.offset
			numFinishedBlobs += currentConsumedBlobs
			currentConsumedBlobs = 0

			if len(srcBlobs) == 0 { // EOF
				return
			}
			lnk, ok = cd.dict[srcBlobs[0]][newOffset]
			if !ok {
				return
			}
		case connectingLink:
			currentConsumedBlobs++
			srcBlobs = srcBlobs[1:]

			if len(srcBlobs) == 0 { // reached EOF
				var nullID restic.ID
				lnk, ok = v[nullID]
				if !ok {
					return
				}
				_ = lnk.(terminalLink)
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

func (cd *ChunkDict) Store(srcBlobs restic.IDs, startOffset, endOffset uint, dstBlob restic.ID) error {
	if len(srcBlobs) == 0 {
		return fmt.Errorf("empty srcBlobs")
	}
	if len(srcBlobs) == 1 && startOffset > endOffset {
		return fmt.Errorf("wrong value. len(srcBlob)==1 and startOffset>endOffset")
	}

	cd.lock.Lock()
	defer cd.lock.Unlock()

	idx, ok := cd.dict[srcBlobs[0]]
	if !ok {
		cd.dict[srcBlobs[0]] = linkIndex{}
		idx = cd.dict[srcBlobs[0]]
	}

	// create link head
	numConnectingLink := len(srcBlobs) - 1
	singleTerminalLink := (numConnectingLink == 0)
	lnk, ok := idx[startOffset]
	if ok { // index exists; type assertion
		if singleTerminalLink {
			_ = lnk.(terminalLink)
			return nil // nothing to touch
		}
		_ = lnk.(connectingLink)
	} else { // index does not exist
		if singleTerminalLink {
			idx[startOffset] = terminalLink{
				dstBlob: dstBlob,
				offset:  endOffset,
			}
			return nil
		}
		idx[startOffset] = connectingLink{}
		lnk = idx[startOffset]
	}
	srcBlobs = srcBlobs[1:]

	// build remaining connectingLink chain
	for range numConnectingLink - 1 {
		c := lnk.(connectingLink)
		lnk, ok = c[srcBlobs[0]]
		if !ok {
			c[srcBlobs[0]] = connectingLink{}
			lnk = c[srcBlobs[0]]
		}
		srcBlobs = srcBlobs[1:]
	}

	// create terminalLink
	c := lnk.(connectingLink)
	lnk, ok = c[srcBlobs[0]]
	if ok { // found that entire chain existed!
		_ = lnk.(terminalLink)
	} else {
		c[srcBlobs[0]] = terminalLink{
			dstBlob: dstBlob,
			offset:  endOffset,
		}
	}

	return nil
}
