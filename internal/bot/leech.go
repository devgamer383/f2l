package bot

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"f2l/internal/security"
	"f2l/internal/stream"
	"f2l/internal/telegram"

	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
)

const (
	DownDir   = "downloads"
	UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

func getLeechPartSize() int64 {
	if env := os.Getenv("LEECH_PART_SIZE_MB"); env != "" {
		if mb, err := strconv.Atoi(env); err == nil && mb >= 500 && mb <= 1950 {
			return int64(mb) * 1024 * 1024
		}
	}
	// Default 1900 MiB slice: fits perfectly within Telegram's 2000 MiB per-file limit with 0 MB disk used
	return 1900 * 1024 * 1024
}

var cdRegex = regexp.MustCompile(`(?i)filename\*?=(?:UTF-8'')?["']?([^"';\r\n]+)`)

func randInt64() int64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return n.Int64()
}

func parseContentRangeTotal(cr string) int64 {
	// Format: bytes 0-1000/10449395486 or bytes */10449395486
	parts := strings.Split(cr, "/")
	if len(parts) == 2 {
		if total, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
			return total
		}
	}
	return 0
}

func extractFilename(rawURL, cd string) string {
	if cd != "" {
		if m := cdRegex.FindStringSubmatch(cd); len(m) > 1 {
			val := strings.Trim(m[1], `"' `)
			if unquoted, err := url.QueryUnescape(val); err == nil && unquoted != "" {
				return unquoted
			}
			return val
		}
	}
	if u, err := url.Parse(rawURL); err == nil {
		base := filepath.Base(u.Path)
		if base != "" && base != "." && base != "/" && strings.Contains(base, ".") {
			if unquoted, err := url.QueryUnescape(base); err == nil && unquoted != "" {
				return unquoted
			}
			return base
		}
	}
	return "downloaded_file.mkv"
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[\\/*?:"<>|]`)
	cleaned := re.ReplaceAllString(name, "_")
	cleaned = strings.ReplaceAll(cleaned, " ", "-")
	if cleaned == "" {
		return "video.mkv"
	}
	return cleaned
}

func joinInts(nums []int) string {
	var parts []string
	for _, n := range nums {
		parts = append(parts, strconv.Itoa(n))
	}
	return strings.Join(parts, ",")
}

type progressReader struct {
	r          io.Reader
	onProgress func(n int64)
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress(int64(n))
	}
	return n, err
}

type uploadProgressTracker struct {
	onProgress func(uploadedBytes int64)
}

func (t *uploadProgressTracker) Chunk(ctx context.Context, state uploader.ProgressState) error {
	if t.onProgress != nil {
		t.onProgress(state.Uploaded)
	}
	return nil
}

func (b *Bot) handleLeech(ctx context.Context, peer tg.InputPeerClass, replyToID int, args []string) {
	if len(args) == 0 {
		b.replyText(ctx, peer, replyToID,
			"ℹ️ **High-Speed 10GB+ Cloud Leech Engine (Go)**\n\n"+
				"**Usage:** `/leech <direct_url> [custom_name.mkv]`\n\n"+
				"• Downloads from any direct HTTP/HTTPS link.\n"+
				"• If file is **> 2 GB**, it automatically slices it into 1.9 GB parts and generates **one single 10 GB stream link**!")
		return
	}

	targetURL := args[0]
	customName := ""
	if len(args) > 1 {
		customName = strings.Join(args[1:], " ")
	}

	slog.Info("Starting leech request", "url", targetURL, "custom_name", customName)

	statusMsgID, err := b.sendReply(ctx, peer, replyToID, "⏳ Connecting to URL and inspecting stream...")
	if err != nil {
		slog.Error("Failed to send initial leech status message", "error", err)
		return
	}
	slog.Info("Sent initial leech status message", "status_msg_id", statusMsgID)


	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   0, // No global timeout for large streaming transfers
	}

	// 1. Inspect URL with HEAD first, fallback to GET Range 0-1024
	inspectCtx, inspectCancel := context.WithTimeout(ctx, 35*time.Second)
	defer inspectCancel()

	headReq, err := http.NewRequestWithContext(inspectCtx, "HEAD", targetURL, nil)
	if err != nil {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Invalid URL: %s", err))
		return
	}
	headReq.Header.Set("User-Agent", UserAgent)
	headReq.Header.Set("Accept", "*/*")

	resp, err := client.Do(headReq)
	var totalSize int64
	var cd string
	supportsRange := false

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength > 0 {
		totalSize = resp.ContentLength
		cd = resp.Header.Get("Content-Disposition")
		if resp.Header.Get("Accept-Ranges") == "bytes" {
			supportsRange = true
		}
		_ = resp.Body.Close()
		slog.Info("HEAD inspection succeeded", "size", totalSize, "supports_range", supportsRange)
	} else {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		slog.Info("HEAD inspection did not return size, trying GET Range 0-1024")

		getReq, gErr := http.NewRequestWithContext(inspectCtx, "GET", targetURL, nil)
		if gErr != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Connection failed: %s", gErr))
			return
		}
		getReq.Header.Set("User-Agent", UserAgent)
		getReq.Header.Set("Range", "bytes=0-1024")
		getReq.Header.Set("Accept", "*/*")

		gResp, gErr := client.Do(getReq)
		if gErr != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Connection failed: %s", gErr))
			return
		}
		defer gResp.Body.Close()

		if gResp.StatusCode != http.StatusOK && gResp.StatusCode != http.StatusPartialContent {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Remote server error: HTTP %d", gResp.StatusCode))
			return
		}

		if gResp.StatusCode == http.StatusPartialContent {
			supportsRange = true
			if cr := gResp.Header.Get("Content-Range"); cr != "" {
				totalSize = parseContentRangeTotal(cr)
			}
		}
		if totalSize <= 0 {
			totalSize = gResp.ContentLength
		}
		cd = gResp.Header.Get("Content-Disposition")
		slog.Info("GET probe completed", "status", gResp.StatusCode, "total_size", totalSize, "supports_range", supportsRange)
	}

	rawFilename := customName
	if rawFilename == "" {
		rawFilename = extractFilename(targetURL, cd)
	}
	filename := sanitizeFilename(rawFilename)

	if totalSize <= 0 {
		b.editStatus(ctx, peer, statusMsgID, "❌ Remote server did not report Content-Length or total file size.")
		return
	}

	availableWorkers := b.pool.GetAvailableWorkers()
	numBots := len(availableWorkers)
	if numBots == 0 {
		numBots = 1
	}

	maxPartSize := getLeechPartSize()
	var numParts int
	if totalSize <= maxPartSize {
		numParts = 1
	} else {
		numParts = int(math.Ceil(float64(totalSize) / float64(maxPartSize)))
	}

	sliceSize := int64(math.Ceil(float64(totalSize) / float64(numParts)))
	cleanName := stream.CleanVirtualName(filename)
	baseURL := strings.TrimRight(b.cfg.Server.BaseURL, "/")

	// Throttled progress updater
	var (
		lastEdit   time.Time
		lastText   string
		progressMu sync.Mutex
	)

	updateProgress := func(text string) {
		progressMu.Lock()
		defer progressMu.Unlock()
		if time.Since(lastEdit) < 3500*time.Millisecond || text == lastText {
			return
		}
		lastEdit = time.Now()
		lastText = text
		b.editStatus(ctx, peer, statusMsgID, text)
	}

	// -------------------------------------------------------------------
	// Case 1: Single Part File (<= 1900 MiB) - 100% Zero-Disk RAM Pipe
	// -------------------------------------------------------------------
	if numParts == 1 {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf(
			"⚡ **Zero-Disk Streaming Leech (Go Engine)**\n\n"+
				"📁 `%s`\n"+
				"📦 **Size:** `%s`\n"+
				"💾 **Disk Usage:** `0 MB (Direct RAM Pipe)`\n"+
				"🚀 **Protocol:** `Direct URL -> Telegram MTProto`",
			filename, telegram.FormatBytes(totalSize),
		))

		dlStart := time.Now()
		var downloaded atomic.Int64

		primary := b.pool.Primary()
		if _, err := b.pool.Resolver().GetInputChannel(ctx, primary.GetAPI()); err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Storage channel error: %s", err))
			return
		}

		var sendRes tg.UpdatesClass
		for attempt := 1; attempt <= 6; attempt++ {
			worker := primary
			if attempt > 1 {
				worker = b.pool.NextWorker()
				time.Sleep(2 * time.Second)
			}
			for !worker.IsReady.Load() {
				time.Sleep(500 * time.Millisecond)
				worker = b.pool.NextWorker()
			}
			downloaded.Store(0)

			dlReq, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
			if err != nil {
				b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Request error: %s", err))
				return
			}
			dlReq.Header.Set("User-Agent", UserAgent)

			dlResp, err := client.Do(dlReq)
			if err != nil || dlResp.StatusCode >= 400 {
				if dlResp != nil {
					_ = dlResp.Body.Close()
				}
				slog.Warn("Single stream connection attempt failed", "attempt", attempt, "err", err)
				continue
			}

			pw := &progressReader{
				r: dlResp.Body,
				onProgress: func(n int64) {
					curr := downloaded.Add(n)
					pText := telegram.RenderProgress("⚡ **Zero-Disk Stream -> Telegram**", filename, curr, totalSize, dlStart, "RAM Pipe (0 MB Disk)")
					updateProgress(pText)
				},
			}

			upload := uploader.NewUpload(filename, pw, totalSize)
			u := uploader.NewUploader(worker.GetAPI()).WithThreads(8).WithPartSize(512 * 1024)
			inputFile, upErr := u.Upload(ctx, upload)
			_ = dlResp.Body.Close()

			if upErr != nil || inputFile == nil {
				slog.Warn("Single stream upload attempt failed", "attempt", attempt, "err", upErr)
				continue
			}

			wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, worker.GetAPI())
			if chErr != nil || wCh == nil {
				slog.Warn("Could not resolve storage channel for worker", "worker_id", worker.ID, "err", chErr)
				continue
			}

			mimeType := mime.TypeByExtension(filepath.Ext(filename))
			if mimeType == "" {
				mimeType = "video/mp4"
			}

			capText, capEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s`\n📦 `%s`", filename, telegram.FormatBytes(totalSize)))
			res, sendErr := worker.GetAPI().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
				Peer: &tg.InputPeerChannel{
					ChannelID:  wCh.ChannelID,
					AccessHash: wCh.AccessHash,
				},
				Media: &tg.InputMediaUploadedDocument{
					File:     inputFile,
					MimeType: mimeType,
					Attributes: []tg.DocumentAttributeClass{
						&tg.DocumentAttributeFilename{FileName: filename},
					},
				},
				Message:  capText,
				Entities: capEntities,
				RandomID: randInt64(),
			})
			if sendErr != nil || res == nil {
				slog.Warn("Single stream send media failed", "attempt", attempt, "err", sendErr)
				continue
			}
			sendRes = res
			break
		}

		if sendRes == nil {
			b.editStatus(ctx, peer, statusMsgID, "❌ Telegram upload failed after 4 attempts.")
			return
		}

		sentMsgID := extractMsgID(sendRes)
		if sentMsgID == 0 {
			b.editStatus(ctx, peer, statusMsgID, "❌ Failed to resolve sent message ID from Telegram.")
			return
		}

		slug := security.EncodeFileSlug(sentMsgID, b.cfg.Telegram.BotToken)
		cleanFilename := telegram.CleanURLFilename(filename)
		streamURL := fmt.Sprintf("%s/inline/%s/%s", baseURL, slug, cleanFilename)
		playerURL := fmt.Sprintf("%s/watch/%s/%s?v=v2", baseURL, slug, cleanFilename)
		downloadURL := fmt.Sprintf("%s/dl/%s/%s", baseURL, slug, cleanFilename)

		successText := fmt.Sprintf(
			"🎬 **Zero-Disk Leech Complete! (Go Fast-Engine)**\n\n"+
				"📁 **File Name:** `%s`\n"+
				"📦 **File Size:** `%s`\n"+
				"💾 **Disk Saved:** `0 MB disk used (100%% direct RAM pipeline)`\n"+
				"⚡ **Stream Protocol:** `RFC 7233 (Zero-Copy Go)`\n\n"+
				"🔗 **Direct Links:**\n"+
				"• **Direct Stream:** [Click Here](%s)\n"+
				"• **Web & VLC Player:** [Click Here](%s)\n"+
				"• **Fast Download:** [Click Here](%s)",
			filename, telegram.FormatBytes(totalSize), streamURL, playerURL, downloadURL,
		)

		markup := createStreamMarkup(streamURL, playerURL, downloadURL)
		b.editStatusWithMarkup(ctx, peer, statusMsgID, successText, markup)
		return
	}

	// -------------------------------------------------------------------
	// Case 2: Multi-Bot Parallel Zero-Disk Leech (> 2GB to 30GB+)
	// -------------------------------------------------------------------
	activeWorkers := numBots
	if activeWorkers > 4 {
		// Limit concurrent slice streams to 4 to prevent remote server HTTP 429 rate limits,
		// while rotating work seamlessly across all 8 bots in the pool
		activeWorkers = 4
	}
	if numParts < activeWorkers {
		activeWorkers = numParts
	}

	modeDesc := "RFC 7233 Parallel Zero-Disk Slicing"
	if !supportsRange {
		modeDesc = "Sequential Zero-Disk Stream Slicing"
	}

	b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf(
		"🚀 **Multi-Bot Zero-Disk Accelerator Engaged! (Go)**\n\n"+
			"📁 `%s` (%s)\n"+
			"🤖 **Worker Bots:** `%d Bots streaming in parallel`\n"+
			"🧩 **Slices:** `%d parts` (~%s each)\n"+
			"💾 **Disk Usage:** `0 MB (Direct RAM Pipe)`\n"+
			"⚡ **Protocol:** `%s`",
		cleanName, telegram.FormatBytes(totalSize), activeWorkers, numParts, telegram.FormatBytes(sliceSize), modeDesc,
	))

	msgIDs := make([]int, numParts)
	partProgress := make([]atomic.Int64, numParts)
	var (
		streamStart time.Time
		streamOnce  sync.Once
	)

	primary := b.pool.Primary()
	storageChannel, err := b.pool.Resolver().GetInputChannel(ctx, primary.GetAPI())
	if err != nil {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Storage channel error: %s", err))
		return
	}

	if supportsRange {
		// Method A: Remote server supports RFC 7233 Range headers
		// All active worker bots stream separate range slices directly to Telegram in parallel with 0 MB disk usage!
		type partJob struct {
			index int
		}
		jobChan := make(chan partJob, numParts)
		for i := 0; i < numParts; i++ {
			jobChan <- partJob{index: i}
		}
		close(jobChan)

		threadsPerWorker := 4
		if activeWorkers <= 2 {
			threadsPerWorker = 8
		}

		var wg sync.WaitGroup
		for wIdx := 0; wIdx < activeWorkers; wIdx++ {
			worker := availableWorkers[wIdx%numBots]
			botNum := (wIdx % numBots) + 1
			wg.Add(1)

			go func(wClient *telegram.WorkerClient, bNum int) {
				defer wg.Done()

				for job := range jobChan {
					select {
					case <-ctx.Done():
						return
					default:
					}

					partIdx := job.index
					partNum := partIdx + 1
					partStart := int64(partIdx) * sliceSize
					partEnd := int64(math.Min(float64(totalSize-1), float64(int64(partIdx+1)*sliceSize-1)))
					partLen := partEnd - partStart + 1
					partFilename := fmt.Sprintf("%s.%03d", cleanName, partNum)

					slog.Info("Starting zero-disk slice stream", "part", partNum, "of", numParts, "start", partStart, "end", partEnd, "len", partLen, "bot", bNum)

					for attempt := 1; attempt <= 6; attempt++ {
						w := wClient
						if attempt > 1 || !w.IsReady.Load() {
							w = b.pool.NextWorker()
							time.Sleep(time.Duration(attempt) * time.Second)
						}
						for !w.IsReady.Load() {
							select {
							case <-ctx.Done():
								return
							case <-time.After(500 * time.Millisecond):
								w = b.pool.NextWorker()
							}
						}
						partProgress[partIdx].Store(0)

						// Connect to URL Range
						pReq, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
						if err != nil {
							slog.Warn("Create Range request failed", "part", partNum, "err", err)
							continue
						}
						pReq.Header.Set("User-Agent", UserAgent)
						pReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partStart, partEnd))

						pResp, err := client.Do(pReq)
						if err != nil || (pResp.StatusCode != http.StatusPartialContent && pResp.StatusCode != http.StatusOK) {
							statusCode := 0
							if pResp != nil {
								statusCode = pResp.StatusCode
								_ = pResp.Body.Close()
							}
							slog.Warn("Range stream connect failed", "part", partNum, "attempt", attempt, "status", statusCode, "err", err)
							backoffSec := attempt * 3
							if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
								backoffSec = attempt * 6
							}
							time.Sleep(time.Duration(backoffSec) * time.Second)
							continue
						}

						var currentPartStreamed int64
						pw := &progressReader{
							r: pResp.Body,
							onProgress: func(n int64) {
								streamOnce.Do(func() { streamStart = time.Now() })
								currentPartStreamed += n
								partProgress[partIdx].Store(currentPartStreamed)
								var curr int64
								for i := range partProgress {
									curr += partProgress[i].Load()
								}
								pText := telegram.RenderProgress("⚡ **Zero-Disk Multi-Bot Accelerator**", cleanName, curr, totalSize, streamStart,
									fmt.Sprintf("Part %d/%d (%d Bots parallel)", partNum, numParts, activeWorkers))
								updateProgress(pText)
							},
						}

						upload := uploader.NewUpload(partFilename, pw, partLen)
						u := uploader.NewUploader(w.GetAPI()).WithThreads(threadsPerWorker).WithPartSize(512 * 1024)
						inputFile, uploadErr := u.Upload(ctx, upload)
						_ = pResp.Body.Close()

						if uploadErr != nil || inputFile == nil {
							slog.Warn("Zero-disk upload slice failed", "part", partNum, "attempt", attempt, "err", uploadErr)
							continue
						}

						wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, w.GetAPI())
						if chErr != nil || wCh == nil {
							slog.Warn("Could not resolve storage channel", "part", partNum, "err", chErr)
							continue
						}

						partCap, partEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", partFilename, partNum, numParts, telegram.FormatBytes(partLen)))
						sRes, sendErr := w.GetAPI().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
							Peer: &tg.InputPeerChannel{
								ChannelID:  wCh.ChannelID,
								AccessHash: wCh.AccessHash,
							},
							Media: &tg.InputMediaUploadedDocument{
								File:     inputFile,
								MimeType: "video/x-matroska",
								Attributes: []tg.DocumentAttributeClass{
									&tg.DocumentAttributeFilename{FileName: partFilename},
								},
							},
							Message:  partCap,
							Entities: partEntities,
							RandomID: randInt64(),
						})
						if sendErr != nil || sRes == nil {
							slog.Warn("Send slice media failed", "part", partNum, "attempt", attempt, "err", sendErr)
							continue
						}

						if msgID := extractMsgID(sRes); msgID > 0 {
							msgIDs[partIdx] = msgID
							slog.Info("Zero-disk slice successfully saved in channel", "part", partNum, "msg_id", msgID)
							break
						}
					}
				}
			}(worker, botNum)
		}
		wg.Wait()

	} else {
		// Method B: Stream sequentially and slice on the fly into parts without writing to disk
		sReq, sErr := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if sErr != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Stream request failed: %s", sErr))
			return
		}
		sReq.Header.Set("User-Agent", UserAgent)

		sResp, err := client.Do(sReq)
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Stream connection failed: %s", err))
			return
		}
		defer sResp.Body.Close()

		for partIdx := 0; partIdx < numParts; partIdx++ {
			select {
			case <-ctx.Done():
				b.editStatus(ctx, peer, statusMsgID, "❌ Stream operation canceled.")
				return
			default:
			}

			partNum := partIdx + 1
			partStart := int64(partIdx) * sliceSize
			partEnd := int64(math.Min(float64(totalSize-1), float64(int64(partIdx+1)*sliceSize-1)))
			partLen := partEnd - partStart + 1
			partFilename := fmt.Sprintf("%s.%03d", cleanName, partNum)

			worker := availableWorkers[partIdx%numBots]
			botTag := fmt.Sprintf("Bot %d/%d", worker.ID, numBots)

			limitR := io.LimitReader(sResp.Body, partLen)
			var currentPartStreamed int64
			pw := &progressReader{
				r: limitR,
				onProgress: func(n int64) {
					streamOnce.Do(func() { streamStart = time.Now() })
					currentPartStreamed += n
					var curr int64
					for i := 0; i < partIdx; i++ {
						curr += partProgress[i].Load()
					}
					curr += currentPartStreamed
					pText := telegram.RenderProgress("⚡ **Zero-Disk Stream -> Telegram**", cleanName, curr, totalSize, streamStart,
						fmt.Sprintf("Part %d/%d (%s)", partNum, numParts, botTag))
					updateProgress(pText)
				},
			}

			upload := uploader.NewUpload(partFilename, pw, partLen)
			u := uploader.NewUploader(worker.GetAPI()).WithThreads(6).WithPartSize(512 * 1024)
			inputFile, uploadErr := u.Upload(ctx, upload)
			if uploadErr != nil || inputFile == nil {
				slog.Warn("Upload stream slice failed", "part", partNum, "err", uploadErr)
				b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Stream slice %d failed: %s", partNum, uploadErr))
				return
			}

			partProgress[partIdx].Store(partLen)

			// 2. Send media to storage channel with retries
			var (
				sRes    tg.UpdatesClass
				sendErr error
			)
			for sendAttempt := 1; sendAttempt <= 4; sendAttempt++ {
				wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, worker.GetAPI())
				if chErr != nil || wCh == nil {
					slog.Warn("Could not resolve storage channel", "part", partNum, "err", chErr)
					time.Sleep(2 * time.Second)
					continue
				}

				pCap, pEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", partFilename, partNum, numParts, telegram.FormatBytes(partLen)))
				sRes, sendErr = worker.GetAPI().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
					Peer: &tg.InputPeerChannel{
						ChannelID:  wCh.ChannelID,
						AccessHash: wCh.AccessHash,
					},
					Media: &tg.InputMediaUploadedDocument{
						File:     inputFile,
						MimeType: "video/x-matroska",
						Attributes: []tg.DocumentAttributeClass{
							&tg.DocumentAttributeFilename{FileName: partFilename},
						},
					},
					Message:  pCap,
					Entities: pEntities,
					RandomID: randInt64(),
				})
				if sendErr == nil && sRes != nil {
					break
				}
				slog.Warn("Send stream slice failed, retrying...", "part", partNum, "attempt", sendAttempt, "err", sendErr)
				time.Sleep(2 * time.Second)
			}

			if sendErr != nil || sRes == nil {
				slog.Warn("Send stream slice failed after retries", "part", partNum, "err", sendErr)
				b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Send slice %d failed after retries: %s", partNum, sendErr))
				return
			}

			if msgID := extractMsgID(sRes); msgID > 0 {
				msgIDs[partIdx] = msgID
				slog.Info("Stream slice successfully saved in channel", "part", partNum, "msg_id", msgID)
			}
		}
	}

	// Bulletproof Recovery Phase: repair any missing slices directly in-RAM before finalizing
	missingIndices := make([]int, 0)
	for i, id := range msgIDs {
		if id <= 0 {
			missingIndices = append(missingIndices, i)
		}
	}

	if len(missingIndices) > 0 {
		slog.Warn("Entering missing slices recovery phase", "missing_count", len(missingIndices))
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf(
			"🔄 **Zero-Disk Recovery Engine Activated**\n\n"+
				"📁 `%s`\n"+
				"🧩 **Repairing:** `%d/%d missing parts`\n"+
				"💾 **Disk Usage:** `0 MB (Direct RAM Pipe)`\n"+
				"⚡ _Finalizing slices into Telegram channel..._",
			cleanName, len(missingIndices), numParts,
		))

		for _, partIdx := range missingIndices {
			select {
			case <-ctx.Done():
				b.editStatus(ctx, peer, statusMsgID, "❌ Leech operation canceled by user.")
				return
			default:
			}

			partNum := partIdx + 1
			partStart := int64(partIdx) * sliceSize
			partEnd := int64(math.Min(float64(totalSize-1), float64(int64(partIdx+1)*sliceSize-1)))
			partLen := partEnd - partStart + 1
			partFilename := fmt.Sprintf("%s.%03d", cleanName, partNum)

			slog.Info("Starting recovery slice stream", "part", partNum, "of", numParts, "start", partStart, "end", partEnd, "len", partLen)

			for recoverAttempt := 1; recoverAttempt <= 8; recoverAttempt++ {
				select {
				case <-ctx.Done():
					b.editStatus(ctx, peer, statusMsgID, "❌ Leech operation canceled by user.")
					return
				default:
				}

				w := b.pool.NextWorker()
				for !w.IsReady.Load() {
					time.Sleep(1 * time.Second)
					w = b.pool.NextWorker()
				}
				partProgress[partIdx].Store(0)

				pReq, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}
				pReq.Header.Set("User-Agent", UserAgent)
				pReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partStart, partEnd))

				pResp, err := client.Do(pReq)
				if err != nil || (pResp.StatusCode != http.StatusPartialContent && pResp.StatusCode != http.StatusOK) {
					statusCode := 0
					if pResp != nil {
						statusCode = pResp.StatusCode
						_ = pResp.Body.Close()
					}
					slog.Warn("Recovery range connect failed", "part", partNum, "attempt", recoverAttempt, "status", statusCode, "err", err)
					backoffSec := recoverAttempt * 4
					if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
						backoffSec = recoverAttempt * 7
					}
					time.Sleep(time.Duration(backoffSec) * time.Second)
					continue
				}

				var currentPartStreamed int64
				pw := &progressReader{
					r: pResp.Body,
					onProgress: func(n int64) {
						currentPartStreamed += n
						partProgress[partIdx].Store(currentPartStreamed)
						var curr int64
						for i := range partProgress {
							curr += partProgress[i].Load()
						}
						pText := telegram.RenderProgress("⚡ **Zero-Disk Recovery Engine**", cleanName, curr, totalSize, streamStart,
							fmt.Sprintf("Repairing Part %d/%d (Bot %d)", partNum, numParts, w.ID))
						updateProgress(pText)
					},
				}

				upload := uploader.NewUpload(partFilename, pw, partLen)
				u := uploader.NewUploader(w.GetAPI()).WithThreads(4).WithPartSize(512 * 1024)
				inputFile, uploadErr := u.Upload(ctx, upload)
				_ = pResp.Body.Close()

				if uploadErr != nil || inputFile == nil {
					slog.Warn("Recovery upload slice failed", "part", partNum, "attempt", recoverAttempt, "err", uploadErr)
					time.Sleep(3 * time.Second)
					continue
				}

				wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, w.GetAPI())
				if chErr != nil || wCh == nil {
					slog.Warn("Could not resolve storage channel during recovery", "part", partNum, "err", chErr)
					time.Sleep(2 * time.Second)
					continue
				}

				partCap, partEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", partFilename, partNum, numParts, telegram.FormatBytes(partLen)))
				sRes, sendErr := w.GetAPI().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
					Peer: &tg.InputPeerChannel{
						ChannelID:  wCh.ChannelID,
						AccessHash: wCh.AccessHash,
					},
					Media: &tg.InputMediaUploadedDocument{
						File:     inputFile,
						MimeType: "video/x-matroska",
						Attributes: []tg.DocumentAttributeClass{
							&tg.DocumentAttributeFilename{FileName: partFilename},
						},
					},
					Message:  partCap,
					Entities: partEntities,
					RandomID: randInt64(),
				})
				if sendErr != nil || sRes == nil {
					slog.Warn("Send recovery slice media failed", "part", partNum, "err", sendErr)
					time.Sleep(2 * time.Second)
					continue
				}

				if msgID := extractMsgID(sRes); msgID > 0 {
					msgIDs[partIdx] = msgID
					slog.Info("Recovery slice successfully saved in channel", "part", partNum, "msg_id", msgID)
					break
				}
			}
		}
	}

	// Final verification that all parts uploaded successfully
	var missingParts []int
	for i, id := range msgIDs {
		if id <= 0 {
			missingParts = append(missingParts, i+1)
		}
	}

	if len(missingParts) > 0 {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Leech aborted: Part(s) %v failed to stream after all recovery attempts.", missingParts))
		return
	}

	validIDs := msgIDs
	if len(validIDs) == 0 {
		b.editStatus(ctx, peer, statusMsgID, "❌ All slice uploads failed.")
		return
	}

	var batchToken string
	part1ID := validIDs[0]
	joinedIDs := joinInts(validIDs)

	if len(validIDs) <= 100 {
		timelineTag := fmt.Sprintf("[timeline:%s]", joinedIDs)
		newCap := fmt.Sprintf("📁 `%s`\n📦 `%s` (%d Parts)\n🎬 `%s`", cleanName, telegram.FormatBytes(totalSize), len(validIDs), timelineTag)
		capText, capEntities := telegram.ParseMarkdown(newCap)
		_, editErr := primary.GetAPI().MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  storageChannel.ChannelID,
				AccessHash: storageChannel.AccessHash,
			},
			ID:       part1ID,
			Message:  capText,
			Entities: capEntities,
		})
		if editErr == nil {
			batchToken = strconv.Itoa(part1ID)
			slog.Info("Part 1 caption updated with timeline tag", "part1_id", part1ID)
		} else {
			slog.Warn("Failed to edit Part 1 caption, falling back to batch token", "err", editErr)
			batchToken = stream.EncodeBatch(validIDs)
		}
	} else {
		manifestData, _ := json.Marshal(map[string]any{
			"type":       "v1_manifest",
			"name":       cleanName,
			"total_size": totalSize,
			"parts":      validIDs,
		})
		sendRes, sErr := primary.GetAPI().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  storageChannel.ChannelID,
				AccessHash: storageChannel.AccessHash,
			},
			Message:  string(manifestData),
			RandomID: randInt64(),
		})
		if sErr == nil {
			manifestID := extractMsgID(sendRes)
			if manifestID > 0 {
				batchToken = strconv.Itoa(manifestID)
			}
		}
		if batchToken == "" {
			batchToken = stream.EncodeBatch(validIDs)
		}
	}

	slug := security.EncodeVirtualSlug(batchToken, b.cfg.Telegram.BotToken)
	cleanUnified := telegram.CleanURLFilename(cleanName)
	streamURL := fmt.Sprintf("%s/inline/v/%s/%s", baseURL, slug, cleanUnified)
	playerURL := fmt.Sprintf("%s/watch/v/%s/%s?v=v2", baseURL, slug, cleanUnified)
	downloadURL := fmt.Sprintf("%s/dl/v/%s/%s", baseURL, slug, cleanUnified)

	successText := fmt.Sprintf(
		"🎬 **Zero-Disk Multi-Bot Leech Complete! (Go Engine)**\n\n"+
			"📁 **Merged Name:** `%s`\n"+
			"📦 **Total Size:** `%s` (%d Parts)\n"+
			"🤖 **Speed Boost:** Uploaded with %d bots streaming simultaneously!\n"+
			"💾 **Disk Saved:** `0 MB disk used (100%% direct RAM pipeline)`\n"+
			"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching`\n\n"+
			"🔗 **Single Seamless Links:**\n"+
			"• **Direct Stream:** [Click Here](%s)\n"+
			"• **Web & VLC Player:** [Click Here](%s)\n"+
			"• **Single Download:** [Click Here](%s)\n\n"+
			"💡 _All %d parts stream and seek seamlessly as one single contiguous file!_",
		cleanName, telegram.FormatBytes(totalSize), len(validIDs), activeWorkers, streamURL, playerURL, downloadURL, len(validIDs),
	)

	markup := createStreamMarkup(streamURL, playerURL, downloadURL)
	b.editStatusWithMarkup(ctx, peer, statusMsgID, successText, markup)
}
