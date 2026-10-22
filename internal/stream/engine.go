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

	// Per-bot file location cache to avoid cross-bot file reference invalidations
	// Key format: fmt.Sprintf("%d:%d", fileID, botID)
	locCache   map[string]*tg.InputDocumentFileLocation
	locCacheMu sync.RWMutex

	// In-flight background warming deduplication
	warming   map[string]bool
	warmingMu sync.Mutex

	// In-flight location resolution deduplication
	resolving   map[string]chan struct{}
	resolvingMu sync.Mutex
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
		locCache:   make(map[string]*tg.InputDocumentFileLocation),
		warming:    make(map[string]bool),
		resolving:  make(map[string]chan struct{}),
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

	// Seed primary bot location in locCache
	if props.Location != nil {
		e.locCacheMu.Lock()
		e.locCache[fmt.Sprintf("%d:%d", messageID, primary.ID)] = props.Location
		e.locCacheMu.Unlock()
	}

	return props, nil
}

// GetLocationForBot retrieves a valid InputDocumentFileLocation tied to the specific bot session
func (e *FastStreamEngine) GetLocationForBot(ctx context.Context, client *telegram.WorkerClient, fileID int, fallbackProps *telegram.FileProperties) (*tg.InputDocumentFileLocation, error) {
	if client == nil || client.GetAPI() == nil || !client.IsReady.Load() {
		client = e.pool.Primary()
	}
	if client == nil || client.GetAPI() == nil {
		return nil, fmt.Errorf("no ready telegram client")
	}

	key := fmt.Sprintf("%d:%d", fileID, client.ID)

	e.locCacheMu.RLock()
	if loc, ok := e.locCache[key]; ok && loc != nil {
		e.locCacheMu.RUnlock()
		return loc, nil
	}
	e.locCacheMu.RUnlock()

	// If primary client and fallbackProps has a location, cache and return immediately (fast-start)
	if client.IsPrimary && fallbackProps != nil && fallbackProps.Location != nil {
		e.locCacheMu.Lock()
		e.locCache[key] = fallbackProps.Location
		e.locCacheMu.Unlock()
		return fallbackProps.Location, nil
	}

	return e.RefreshLocationForBot(ctx, client, fileID)
}

// RefreshLocationForBot fetches a fresh, session-bound FileReference for a specific bot client
func (e *FastStreamEngine) RefreshLocationForBot(ctx context.Context, client *telegram.WorkerClient, fileID int) (*tg.InputDocumentFileLocation, error) {
	if client == nil || client.GetAPI() == nil || !client.IsReady.Load() {
		return nil, fmt.Errorf("bot client not ready")
	}

	key := fmt.Sprintf("%d:%d", fileID, client.ID)

	e.resolvingMu.Lock()
	if waitCh, inProgress := e.resolving[key]; inProgress {
		e.resolvingMu.Unlock()
		select {
		case <-waitCh:
			if loc := e.GetCachedLocation(fileID, client.ID); loc != nil {
				return loc, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ch := make(chan struct{})
	e.resolving[key] = ch
	e.resolvingMu.Unlock()

	defer func() {
		e.resolvingMu.Lock()
		delete(e.resolving, key)
		close(ch)
		e.resolvingMu.Unlock()
	}()

	inCh, err := e.pool.Resolver().GetInputChannel(ctx, client.GetAPI())
	if err != nil {
		return nil, fmt.Errorf("resolve storage channel for bot %d: %w", client.ID, err)
	}

	res, err := client.GetAPI().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: inCh,
		ID: []tg.InputMessageClass{
			&tg.InputMessageID{ID: fileID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get message %d on bot %d: %w", fileID, client.ID, err)
	}

	modified, ok := res.AsModified()
	if !ok || len(modified.GetMessages()) == 0 {
		return nil, fmt.Errorf("message %d not found on bot %d", fileID, client.ID)
	}

	msg, ok := modified.GetMessages()[0].(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("message %d invalid on bot %d", fileID, client.ID)
	}

	props, err := telegram.GetFileProperties(msg)
	if err != nil {
		return nil, err
	}

	e.locCacheMu.Lock()
	e.locCache[key] = props.Location
	e.locCacheMu.Unlock()

	if client.IsPrimary {
		e.msgCacheMu.Lock()
		e.msgCache[fileID] = props
		e.msgCacheMu.Unlock()
	}

	return props.Location, nil
}

// GetCachedLocation returns the cached location for (fileID, botID) if present
func (e *FastStreamEngine) GetCachedLocation(fileID, botID int) *tg.InputDocumentFileLocation {
	key := fmt.Sprintf("%d:%d", fileID, botID)
	e.locCacheMu.RLock()
	defer e.locCacheMu.RUnlock()
	return e.locCache[key]
}

// AsyncWarmLocation resolves and caches the location for a secondary bot in the background
func (e *FastStreamEngine) AsyncWarmLocation(client *telegram.WorkerClient, fileID int) {
	if client == nil || client.IsPrimary || client.GetAPI() == nil || !client.IsReady.Load() {
		return
	}
	key := fmt.Sprintf("%d:%d", fileID, client.ID)

	e.warmingMu.Lock()
	if e.warming[key] {
		e.warmingMu.Unlock()
		return
	}
	e.warming[key] = true
	e.warmingMu.Unlock()

	go func() {
		defer func() {
			e.warmingMu.Lock()
			delete(e.warming, key)
			e.warmingMu.Unlock()
		}()

		bgCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_, _ = e.RefreshLocationForBot(bgCtx, client, fileID)
	}()
}

// RefreshFileProperties refreshes primary properties
func (e *FastStreamEngine) RefreshFileProperties(ctx context.Context, messageID int) (*telegram.FileProperties, error) {
	primary := e.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return nil, fmt.Errorf("primary bot not ready")
	}
	loc, err := e.RefreshLocationForBot(ctx, primary, messageID)
	if err != nil {
		return nil, err
	}
	e.msgCacheMu.RLock()
	props := e.msgCache[messageID]
	e.msgCacheMu.RUnlock()
	if props != nil {
		props.Location = loc
		return props, nil
	}
	return e.GetFileProperties(ctx, messageID)
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

	// Check cache if single chunk metadata probe (e.g. video player moov atom or cues)
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

	// Select verified download workers (bots ready AND have confirmed channel access)
	availableWorkers := e.pool.GetDownloadWorkers()
	if len(availableWorkers) == 0 {
		if primary := e.pool.Primary(); primary != nil && primary.IsReady.Load() {
			availableWorkers = []*telegram.WorkerClient{primary}
		} else {
			return fmt.Errorf("no ready telegram client available")
		}
	}
	numWorkers := len(availableWorkers)

	// Determine concurrency:
	// If 1 bot, pipeline 4-8 chunk requests on its socket for high throughput.
	// If multiple bots, distribute 1 worker goroutine per bot up to min(numWorkers, 8).
	concurrency := numWorkers
	if concurrency == 1 {
		concurrency = 4
		if e.prefetch >= 4 && e.prefetch <= 8 {
			concurrency = e.prefetch
		}
	} else if concurrency > 8 {
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

	// Pipelined workers: multiplex in-flight chunk requests
	for w := 0; w < concurrency; w++ {
		workerBot := availableWorkers[w%numWorkers]
		wg.Add(1)
		go func(assignedBot *telegram.WorkerClient) {
			defer wg.Done()
			activeBot := assignedBot

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

				// 2. Instant fast-path: use warm cached location, or immediately fallback to Primary without blocking!
				loc := e.GetCachedLocation(fileID, activeBot.ID)
				if loc == nil {
					if !activeBot.IsPrimary {
						e.AsyncWarmLocation(activeBot, fileID)
						activeBot = e.pool.Primary()
					}
					loc = e.GetCachedLocation(fileID, activeBot.ID)
					if loc == nil && props != nil && props.Location != nil {
						loc = props.Location
						e.locCacheMu.Lock()
						e.locCache[fmt.Sprintf("%d:%d", fileID, activeBot.ID)] = loc
						e.locCacheMu.Unlock()
					}
				}
				if loc == nil {
					var locErr error
					loc, locErr = e.GetLocationForBot(workerCtx, activeBot, fileID, props)
					if locErr != nil {
						select {
						case resultChan <- streamChunk{index: idx, err: locErr}:
						case <-workerCtx.Done():
						}
						return
					}
				}

				// 3. Fetch chunk from Telegram MTProto on this bot's socket
				chunkOffsetBytes := curOffsetChunk * chunkSize
				req := &tg.UploadGetFileRequest{
					Location: loc,
					Offset:   chunkOffsetBytes,
					Limit:    int(chunkSize),
					Precise:  true,
				}

				var res tg.UploadFileClass
				var fetchErr error

				for attempt := 1; attempt <= 5; attempt++ {
					if activeBot == nil || !activeBot.IsReady.Load() || activeBot.GetAPI() == nil {
						activeBot = e.pool.Primary()
					}
					if activeBot != nil && activeBot.GetAPI() != nil {
						res, fetchErr = activeBot.GetAPI().UploadGetFile(workerCtx, req)
						if fetchErr == nil && res != nil {
							break
						}
					} else {
						fetchErr = fmt.Errorf("no ready bot client")
					}
					if workerCtx.Err() != nil {
						return
					}

					// 1. Handle Telegram FLOOD_WAIT with intelligent backoff & worker rotation
					if fetchErr != nil && strings.Contains(fetchErr.Error(), "FLOOD_WAIT") {
						waitSec := 2
						if idx := strings.Index(fetchErr.Error(), "FLOOD_WAIT"); idx != -1 {
							sub := fetchErr.Error()[idx:]
							var parsedSec int
							if _, sErr := fmt.Sscanf(sub, "FLOOD_WAIT (%d)", &parsedSec); sErr == nil && parsedSec > 0 && parsedSec <= 15 {
								waitSec = parsedSec
							}
						}
						slog.Warn("Telegram FLOOD_WAIT received, backing off...", "bot_id", activeBot.ID, "wait_sec", waitSec, "chunk", curOffsetChunk)
						select {
						case <-time.After(time.Duration(waitSec) * time.Second):
						case <-workerCtx.Done():
							return
						}
						// Rotate to another ready worker bot if available
						if alt := e.pool.NextWorker(); alt != nil && alt != activeBot {
							activeBot = alt
							if altLoc, lErr := e.GetLocationForBot(workerCtx, activeBot, fileID, props); lErr == nil && altLoc != nil {
								loc = altLoc
								req.Location = altLoc
							}
						}
						continue
					}

					// 2. Auto-refresh expired Telegram file reference specifically for activeBot
					if fetchErr != nil && strings.Contains(fetchErr.Error(), "FILE_REFERENCE_EXPIRED") {
						slog.Info("Telegram file reference expired, refreshing...", "file_id", fileID, "bot_id", activeBot.ID, "chunk", curOffsetChunk)
						freshLoc, refErr := e.RefreshLocationForBot(workerCtx, activeBot, fileID)
						if refErr == nil && freshLoc != nil {
							loc = freshLoc
							req.Location = freshLoc
							continue
						}
						// If secondary bot cannot refresh, switch to Primary bot
						if !activeBot.IsPrimary {
							activeBot = e.pool.Primary()
							if activeBot != nil {
								if pLoc, pErr := e.GetLocationForBot(workerCtx, activeBot, fileID, props); pErr == nil && pLoc != nil {
									loc = pLoc
									req.Location = pLoc
									continue
								}
							}
						}
					}

					select {
					case <-time.After(150 * time.Millisecond):
					case <-workerCtx.Done():
						return
					}
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
