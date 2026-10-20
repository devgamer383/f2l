package stream

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"f2l/internal/cache"
	"f2l/internal/telegram"

	"github.com/gotd/td/tg"
)

type FastStreamEngine struct {
	pool      *telegram.BotPool
	chunkMem  *cache.LRUCache
	chunkSize int
	prefetch  int

	// Cached message metadata to avoid MTProto lookups on seek
	msgCache   map[int]*telegram.FileProperties
	msgCacheMu sync.RWMutex
}

func NewFastStreamEngine(pool *telegram.BotPool, chunkCache *cache.LRUCache, chunkSize, prefetch int) *FastStreamEngine {
	if chunkSize <= 0 {
		chunkSize = 1024 * 1024 // 1 MB
	}
	if prefetch < 2 {
		prefetch = 4
	}
	return &FastStreamEngine{
		pool:       pool,
		chunkMem:   chunkCache,
		chunkSize:  chunkSize,
		prefetch:   prefetch,
		msgCache:   make(map[int]*telegram.FileProperties),
	}
}

func (e *FastStreamEngine) GetFileProperties(ctx context.Context, messageID int) (*telegram.FileProperties, error) {
	e.msgCacheMu.RLock()
	if props, found := e.msgCache[messageID]; found {
		e.msgCacheMu.RUnlock()
		return props, nil
	}
	e.msgCacheMu.RUnlock()

	primary := e.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return nil, fmt.Errorf("telegram client is not ready")
	}

	ch, err := e.pool.Resolver().GetInputChannel(ctx, primary.API)
	if err != nil {
		return nil, fmt.Errorf("resolve storage channel: %w", err)
	}

	res, err := primary.API.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID: []tg.InputMessageClass{
			&tg.InputMessageID{ID: messageID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get message %d: %w", messageID, err)
	}

	modified, ok := res.AsModified()
	if !ok {
		return nil, fmt.Errorf("message %d returned unmodified", messageID)
	}
	messages := modified.GetMessages()
	if len(messages) == 0 {
		return nil, fmt.Errorf("message %d not found", messageID)
	}

	msg, ok := messages[0].(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("message %d is not a valid message", messageID)
	}

	props, err := telegram.GetFileProperties(msg)
	if err != nil {
		return nil, err
	}

	e.msgCacheMu.Lock()
	e.msgCache[messageID] = props
	e.msgCacheMu.Unlock()

	return props, nil
}

type chunkItem struct {
	index int
	data  []byte
	err   error
}

func (e *FastStreamEngine) StreamFileRange(
	ctx context.Context,
	w http.ResponseWriter,
	fileID int,
	startByte, endByte, contentLength int64,
	cachedProps *telegram.FileProperties,
) error {
	var props *telegram.FileProperties
	var err error

	if cachedProps != nil {
		props = cachedProps
	} else {
		props, err = e.GetFileProperties(ctx, fileID)
		if err != nil {
			return err
		}
	}

	chunkSize := int64(e.chunkSize)
	offsetChunk := startByte / chunkSize
	totalBytes := endByte - startByte + 1
	chunksToStream := int(math.Ceil(float64(totalBytes) / float64(chunkSize)))

	// Check cache if single chunk metadata probe (e.g. video player moov atom)
	cacheKey := fmt.Sprintf("%d_%d", fileID, offsetChunk)
	if chunksToStream == 1 {
		if cached, ok := e.chunkMem.Get(cacheKey); ok {
			trimStart := startByte % chunkSize
			chunk := cached[trimStart:]
			if int64(len(chunk)) > contentLength {
				chunk = chunk[:contentLength]
			}
			if len(chunk) > 0 {
				_, _ = w.Write(chunk)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			return nil
		}
	}

	// Select ready client: Primary bot has the authenticated MTProto session
	botClient := e.pool.Primary()
	if botClient == nil || !botClient.IsReady.Load() {
		botClient = e.pool.NextWorker()
	}
	if botClient == nil {
		return fmt.Errorf("no ready telegram client available")
	}

	prefetch := 4
	if e.prefetch >= 2 && e.prefetch <= 8 {
		prefetch = e.prefetch
	}
	if chunksToStream < prefetch {
		prefetch = chunksToStream
	}

	type streamChunk struct {
		index int
		data  []byte
		err   error
	}

	chunkChan := make(chan streamChunk, prefetch)
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// Producer goroutine: continuously pulls chunks sequentially ahead of player on same warm connection!
	go func() {
		defer close(chunkChan)

		for idx := 0; idx < chunksToStream; idx++ {
			select {
			case <-workerCtx.Done():
				return
			default:
			}

			curOffsetChunk := offsetChunk + int64(idx)
			curCacheKey := fmt.Sprintf("%d_%d", fileID, curOffsetChunk)

			// 1. Check in-memory LRU cache
			if cached, ok := e.chunkMem.Get(curCacheKey); ok {
				select {
				case chunkChan <- streamChunk{index: idx, data: cached}:
				case <-workerCtx.Done():
					return
				}
				continue
			}

			// 2. Fetch chunk from Telegram MTProto on persistent connection
			chunkOffsetBytes := curOffsetChunk * chunkSize
			req := &tg.UploadGetFileRequest{
				Location: props.Location,
				Offset:   chunkOffsetBytes,
				Limit:    int(chunkSize),
				Precise:  true,
			}

			var res tg.UploadFileClass
			var fetchErr error

			for attempt := 1; attempt <= 3; attempt++ {
				clientToUse := botClient
				if attempt > 1 {
					if primary := e.pool.Primary(); primary != nil {
						clientToUse = primary
					}
					time.Sleep(150 * time.Millisecond)
				}

				res, fetchErr = clientToUse.API.UploadGetFile(workerCtx, req)
				if fetchErr == nil && res != nil {
					break
				}
				if workerCtx.Err() != nil {
					return
				}
			}

			if fetchErr != nil || res == nil {
				if workerCtx.Err() == nil {
					slog.Warn("UploadGetFile failed", "file_id", fileID, "chunk", curOffsetChunk, "err", fetchErr)
					select {
					case chunkChan <- streamChunk{index: idx, err: fetchErr}:
					case <-workerCtx.Done():
					}
				}
				return
			}

			var chunkBytes []byte
			switch v := res.(type) {
			case *tg.UploadFile:
				chunkBytes = v.Bytes
			case *tg.UploadFileCDNRedirect:
				select {
				case chunkChan <- streamChunk{index: idx, err: fmt.Errorf("CDN redirect not supported")}:
				case <-workerCtx.Done():
				}
				return
			default:
				select {
				case chunkChan <- streamChunk{index: idx, err: fmt.Errorf("unexpected file response")}:
				case <-workerCtx.Done():
				}
				return
			}

			// Store in LRU cache
			e.chunkMem.Set(curCacheKey, chunkBytes)

			select {
			case chunkChan <- streamChunk{index: idx, data: chunkBytes}:
			case <-workerCtx.Done():
				return
			}
		}
	}()

	// Consumer loop: write chunks sequentially to http.ResponseWriter with immediate flush
	var bytesStreamed int64 = 0
	flusher, hasFlusher := w.(http.Flusher)

	for item := range chunkChan {
		if item.err != nil {
			return item.err
		}

		chunk := item.data
		if item.index == 0 {
			trimStart := startByte % chunkSize
			if trimStart > 0 && trimStart < int64(len(chunk)) {
				chunk = chunk[trimStart:]
			}
		}

		remaining := contentLength - bytesStreamed
		if remaining <= 0 {
			break
		}
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}

		if len(chunk) > 0 {
			n, err := w.Write(chunk)
			if err != nil {
				return err // Player disconnected or seeked
			}
			bytesStreamed += int64(n)
			if hasFlusher {
				flusher.Flush()
			}
		}

		if bytesStreamed >= contentLength {
			break
		}
	}

	return nil
}
