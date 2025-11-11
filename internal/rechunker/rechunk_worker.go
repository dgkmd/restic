package rechunker

import (
	"context"
	"errors"
	"io"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

type ChunkedFile struct {
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
type blobLoadCallbackFn func(ids restic.IDs) error

var ErrNewStream = errors.New("new stream")

func createBlobLoadCallback(ctx context.Context, c *BlobCache, idx *RechunkerIndex) blobLoadCallbackFn {
	if c == nil {
		return nil
	}

	return func(ids restic.IDs) error {
		var ignoresList restic.IDs
		idx.blobRemainingLock.Lock()
		for _, blob := range ids {
			idx.blobRemaining[blob]--
			if idx.blobRemaining[blob] == 0 {
				ignoresList = append(ignoresList, blob)
			}
		}
		idx.blobRemainingLock.Unlock()

		if len(ignoresList) > 0 {
			return c.Ignore(ctx, ignoresList)
		}

		return nil
	}
}

func prioritySelect(ctx context.Context, chFirst <-chan *ChunkedFile, chSecond <-chan *ChunkedFile) (file *ChunkedFile, ok bool, err error) {
	if chSecond != nil {
		// Firstly, try chFirst only. If chFirst is not ready now, wait for both chFirst and chSecond.
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case file, ok = <-chFirst:
		default:
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case file, ok = <-chFirst:
			case file, ok = <-chSecond:
			}
		}
	} else {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case file, ok = <-chFirst:
		}
	}

	return file, ok, nil
}

func prepareChunkDict(ctx context.Context, wg *errgroup.Group, srcBlobs restic.IDs, d *ChunkDict, idx *RechunkerIndex) (info *ChunkedFileContext, ff *fastForward, prefix restic.IDs) {
	// build blobPos (position of each blob in a file)
	blobPos := make([]uint, len(srcBlobs)+1)
	var offset uint
	for i, blob := range srcBlobs {
		offset += idx.blobSize[blob]
		blobPos[i+1] = offset
	}
	if blobPos[1] == 0 { // assertion
		panic("blobPos not computed correctly")
	}

	// define seekBlobPos
	seekBlobPos := func(pos uint, seekStartIdx int) (int, uint) {
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
	prefix, numFinishedBlobs, newOffset := d.Match(srcBlobs, 0)
	var prefixIdx int
	var prefixPos uint
	if numFinishedBlobs > 0 {
		// debugNote: record chunkdict prefix match event
		debug.Log("ChunkDict prefix match at %v: Skipping %d blobs", srcBlobs[0].Str(), numFinishedBlobs)
		debugNoteLock.Lock()
		debugNote["chunkdict_event"]++
		debugNote["chunkdict_blob_count"] += numFinishedBlobs
		debugNoteLock.Unlock()

		prefixIdx = numFinishedBlobs
		prefixPos = blobPos[prefixIdx] + newOffset

		ff = &fastForward{
			newStNum: 0,
			blobIdx:  prefixIdx,
			offset:   newOffset,
		}
	}

	info = &ChunkedFileContext{
		srcBlobs:    srcBlobs,
		blobPos:     blobPos,
		seekBlobPos: seekBlobPos,
		dictStore:   d.Store,
		dictMatch:   d.Match,
		prefixPos:   prefixPos,
		prefixIdx:   prefixIdx,
	}

	return info, ff, prefix
}

func startFileStreamer(ctx context.Context, wg *errgroup.Group, srcBlobs restic.IDs, out chan<- fileStreamReader, getBlob getBlobFn, onBlobLoad blobLoadCallbackFn, bufferPool chan []byte) {
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
			buf, err := getBlob(srcBlobs[i], buf, nil)
			if err != nil {
				return err
			}
			if onBlobLoad != nil {
				onBlobLoad(srcBlobs[i : i+1])
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

func startFileStreamerWithFastForward(ctx context.Context, wg *errgroup.Group, srcBlobs restic.IDs, out chan<- fileStreamReader, getBlob getBlobFn, onBlobLoad blobLoadCallbackFn, bufferPool chan []byte, ff <-chan fastForward) {
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
				if onBlobLoad != nil {
					onBlobLoad(srcBlobs[i:ffPos.blobIdx])
				}
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
			buf, err := getBlob(srcBlobs[i], buf, nil)
			if err != nil {
				return err
			}
			if offset != 0 {
				copy(buf, buf[offset:])
				buf = buf[:len(buf)-int(offset)]
				offset = 0
			}
			if onBlobLoad != nil {
				onBlobLoad(srcBlobs[i : i+1])
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
				debug.Log("Received NewStream signal; preparing new reader")
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

func startFileBlobSaverWithFastForward(ctx context.Context, wg *errgroup.Group, in <-chan chunk, out chan<- restic.IDs, dstRepo restic.BlobSaver, bufferPool chan<- []byte, ff chan<- fastForward, info *ChunkedFileContext) {
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
