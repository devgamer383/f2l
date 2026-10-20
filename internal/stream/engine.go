package stream

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"

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

	// DC sub-client invoker cache
	dcInvokers   map[int]tg.Invoker
	dcInvokersMu sync.Mutex
}

func NewFastStreamEngine(pool *telegram.BotPool, chunkCache *cache.LRUCache, chunkSize, prefetch int) *FastStreamEngine {
	if chunkSize <= 0 {
		chunkSize = 1024 * 1024 // 1 MB
	}
	if prefetch < 2 {
		prefetch = 4
	}
	return &FastStreamEngine{
		pool:        pool,
		chunkMem:    chunkCache,
		chunkSize:   chunkSize,
		prefetch:    prefetch,
		msgCache:    make(map[int]*telegram.FileProperties),
		dcInvokers:  make(map[int]tg.Invoker),
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

func (e *FastStreamEngine) getInvokerForDC(ctx context.Context, dcID int) tg.Invoker {
	primary := e.pool.Primary()
	if primary == nil {
		return nil
	}

	if dcID <= 0 {
		return primary.Client
	}

	e.dcInvokersMu.Lock()
	defer e.dcInvokersMu.Unlock()

	if inv, found := e.dcInvokers[dcID]; found {
		return inv
	}

	// Connect directly to media-only DC cluster
	inv, err := primary.Client.MediaOnly(ctx, dcID, 5)
	if err != nil {
		slog.Warn("Failed to create MediaOnly invoker, falling back to primary Client", "dc", dcID, "err", err)
		return primary.Client
	}

	e.dcInvokers[dcID] = inv
	return inv
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

	invoker := e.getInvokerForDC(ctx, props.DCID)
	api := tg.NewClient(invoker)

	// Determine worker concurrency for parallel chunk download
	concurrency := 4
	if e.prefetch > concurrency {
		concurrency = e.prefetch
	}
	if chunksToStream < concurrency {
		concurrency = chunksToStream
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	taskChan := make(chan int, chunksToStream)
	resultChan := make(chan chunkItem, e.prefetch*2)

	for i := 0; i < chunksToStream; i++ {
		taskChan <- i
	}
	close(taskChan)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range taskChan {
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				curOffsetChunk := offsetChunk + int64(idx)
				curCacheKey := fmt.Sprintf("%d_%d", fileID, curOffsetChunk)

				// Fast path: In-memory cache hit
				if cached, ok := e.chunkMem.Get(curCacheKey); ok {
					select {
					case resultChan <- chunkItem{index: idx, data: cached}:
					case <-workerCtx.Done():
					}
					continue
				}

				// Network fetch
				chunkOffsetBytes := curOffsetChunk * chunkSize
				req := &tg.UploadGetFileRequest{
					Location: props.Location,
					Offset:   chunkOffsetBytes,
					Limit:    int(chunkSize),
					Precise:  true,
				}

				res, err := api.UploadGetFile(workerCtx, req)
				if err != nil {
					select {
					case resultChan <- chunkItem{index: idx, err: err}:
					case <-workerCtx.Done():
					}
					return
				}

				var chunkBytes []byte
				switch v := res.(type) {
				case *tg.UploadFile:
					chunkBytes = v.Bytes
				case *tg.UploadFileCDNRedirect:
					select {
					case resultChan <- chunkItem{index: idx, err: fmt.Errorf("CDN redirect not supported")}:
					case <-workerCtx.Done():
					}
					return
				default:
					select {
					case resultChan <- chunkItem{index: idx, err: fmt.Errorf("unexpected UploadFile response type")}:
					case <-workerCtx.Done():
					}
					return
				}

				// Store in LRU cache
				e.chunkMem.Set(curCacheKey, chunkBytes)

				select {
				case resultChan <- chunkItem{index: idx, data: chunkBytes}:
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	// Close resultChan when all workers finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var bytesStreamed int64 = 0
	expectedIdx := 0
	flusher, hasFlusher := w.(http.Flusher)
	pendingChunks := make(map[int][]byte)

	for {
		if expectedIdx >= chunksToStream || bytesStreamed >= contentLength {
			break
		}

		// Check if next in-order chunk is already available in pending buffer
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
					return err // Client closed the connection
				}
				bytesStreamed += int64(n)
				if hasFlusher {
					flusher.Flush()
				}
			}

			expectedIdx++
			continue
		}

		// Wait for next result from parallel workers
		item, ok := <-resultChan
		if !ok {
			break // No more items
		}
		if item.err != nil {
			return item.err
		}

		pendingChunks[item.index] = item.data
	}

	return nil
}
