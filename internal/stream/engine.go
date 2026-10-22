package stream

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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

	ch, err := e.pool.Resolver().GetInputChannel(ctx, primary.GetAPI())
	if err != nil {
		return nil, fmt.Errorf("resolve storage channel: %w", err)
	}

	res, err := primary.GetAPI().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
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

// RefreshFileProperties bypasses the cache to fetch fresh FileReferences from Telegram
func (e *FastStreamEngine) RefreshFileProperties(ctx context.Context, messageID int) (*telegram.FileProperties, error) {
	return e.RefreshFilePropertiesClient(ctx, nil, messageID)
}

// RefreshFilePropertiesClient fetches fresh FileReferences specifically bound to the client's session
func (e *FastStreamEngine) RefreshFilePropertiesClient(ctx context.Context, client *telegram.WorkerClient, messageID int) (*telegram.FileProperties, error) {
	if client == nil || client.GetAPI() == nil || !client.IsReady.Load() {
		client = e.pool.Primary()
		if client == nil || !client.IsReady.Load() {
			client = e.pool.NextWorker()
		}
	}
	if client == nil || client.GetAPI() == nil {
		return nil, fmt.Errorf("no ready telegram client available")
	}

	ch, err := e.pool.Resolver().GetInputChannel(ctx, client.GetAPI())
	if err != nil {
		primary := e.pool.Primary()
		if primary != nil && primary.IsReady.Load() && primary != client {
			if pch, perr := e.pool.Resolver().GetInputChannel(ctx, primary.GetAPI()); perr == nil {
				client = primary
				ch = pch
			} else {
				return nil, fmt.Errorf("resolve storage channel: %w", err)
			}
		} else {
			return nil, fmt.Errorf("resolve storage channel: %w", err)
		}
	}

	res, err := client.GetAPI().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID: []tg.InputMessageClass{
			&tg.InputMessageID{ID: messageID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("refresh message %d: %w", messageID, err)
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
	startChunk := startByte / chunkSize
	endChunk := endByte / chunkSize
	chunksToStream := int(endChunk - startChunk + 1)
	offsetChunk := startChunk

	// Check cache if single chunk metadata probe (e.g. video player moov atom)
	if startChunk == endChunk {
		cacheKey := fmt.Sprintf("%d_%d", fileID, startChunk)
		if cached, ok := e.chunkMem.Get(cacheKey); ok {
			trimStart := startByte % chunkSize
			if trimStart < int64(len(cached)) {
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
	}

	// Select ready workers: stripe chunk requests across all ready bots in the pool
	availableWorkers := e.pool.GetAvailableWorkers()
	if len(availableWorkers) == 0 {
		if primary := e.pool.Primary(); primary != nil && primary.IsReady.Load() {
			availableWorkers = []*telegram.WorkerClient{primary}
		} else {
			return fmt.Errorf("no ready telegram client available")
		}
	}
	numWorkers := len(availableWorkers)

	concurrency := numWorkers
	if e.prefetch > concurrency {
		concurrency = e.prefetch
	}
	if concurrency > 8 {
		concurrency = 8
	}
	if chunksToStream < concurrency {
		concurrency = chunksToStream
	}

	type streamChunk struct {
		index int
		data  []byte
		err   error
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// Task channel with bounded buffer for smooth continuous prefetching ahead of the player
	taskChan := make(chan int, concurrency*2)
	go func() {
		defer close(taskChan)
		for nextChunk := 0; nextChunk < chunksToStream; nextChunk++ {
			select {
			case <-workerCtx.Done():
				return
			case taskChan <- nextChunk:
			}
		}
	}()

	resultChan := make(chan streamChunk, concurrency*2)
	var wg sync.WaitGroup

	// Pipelined workers: multiplex in-flight chunk requests across distinct worker bot connections
	for w := 0; w < concurrency; w++ {
		workerBot := availableWorkers[w%numWorkers]
		wg.Add(1)
		go func(wClient *telegram.WorkerClient) {
			defer wg.Done()
			for idx := range taskChan {
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
					case resultChan <- streamChunk{index: idx, data: cached}:
					case <-workerCtx.Done():
					}
					continue
				}

				// 2. Fetch chunk from Telegram MTProto on this worker's dedicated socket
				chunkOffsetBytes := curOffsetChunk * chunkSize
				req := &tg.UploadGetFileRequest{
					Location: props.Location,
					Offset:   chunkOffsetBytes,
					Limit:    int(chunkSize),
					Precise:  true,
				}

				var res tg.UploadFileClass
				var fetchErr error

				activeClient := wClient
				for attempt := 1; attempt <= 3; attempt++ {
					if activeClient == nil || !activeClient.IsReady.Load() || activeClient.GetAPI() == nil {
						activeClient = e.pool.NextWorker()
						if activeClient == nil || !activeClient.IsReady.Load() {
							activeClient = e.pool.Primary()
						}
					}
					if activeClient != nil && activeClient.GetAPI() != nil {
						res, fetchErr = activeClient.GetAPI().UploadGetFile(workerCtx, req)
						if fetchErr == nil && res != nil {
							break
						}
					} else {
						fetchErr = fmt.Errorf("no ready bot client")
					}
					if workerCtx.Err() != nil {
						return
					}

					// Auto-refresh expired Telegram file references directly for activeClient
					if fetchErr != nil && strings.Contains(fetchErr.Error(), "FILE_REFERENCE_EXPIRED") {
						slog.Info("Telegram file reference expired, fetching fresh reference...", "file_id", fileID, "bot_id", activeClient.ID, "chunk", curOffsetChunk)
						freshProps, refErr := e.RefreshFilePropertiesClient(workerCtx, activeClient, fileID)
						if refErr == nil && freshProps != nil && freshProps.Location != nil {
							req.Location = freshProps.Location
							props.Location = freshProps.Location
							props.Doc = freshProps.Doc
							// Retry immediately with fresh reference
							continue
						}
					}

					// Rotate on retry
					activeClient = e.pool.NextWorker()
					time.Sleep(100 * time.Millisecond)
				}

				if fetchErr != nil || res == nil {
					if workerCtx.Err() == nil && !strings.Contains(fetchErr.Error(), "context canceled") {
						slog.Warn("UploadGetFile failed", "file_id", fileID, "chunk", curOffsetChunk, "err", fetchErr)
						select {
						case resultChan <- streamChunk{index: idx, err: fetchErr}:
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
					case resultChan <- streamChunk{index: idx, err: fmt.Errorf("CDN redirect not supported")}:
					case <-workerCtx.Done():
					}
					return
				default:
					select {
					case resultChan <- streamChunk{index: idx, err: fmt.Errorf("unexpected file response")}:
					case <-workerCtx.Done():
					}
					return
				}

				// Store in LRU cache
				e.chunkMem.Set(curCacheKey, chunkBytes)

				select {
				case resultChan <- streamChunk{index: idx, data: chunkBytes}:
				case <-workerCtx.Done():
					return
				}
			}
		}(workerBot)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Consumer loop: reassemble in exact numerical order and write with immediate flush
	var bytesStreamed int64 = 0
	flusher, hasFlusher := w.(http.Flusher)
	expectedIdx := 0
	pendingChunks := make(map[int][]byte)

	for {
		if expectedIdx >= chunksToStream || bytesStreamed >= contentLength {
			break
		}

		if chunk, exists := pendingChunks[expectedIdx]; exists {
			delete(pendingChunks, expectedIdx)

			if expectedIdx == 0 {
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

			expectedIdx++
			continue
		}

		// Wait for next completed chunk
		item, ok := <-resultChan
		if !ok {
			break
		}
		if item.err != nil {
			return item.err
		}
		pendingChunks[item.index] = item.data
	}

	return nil
}
