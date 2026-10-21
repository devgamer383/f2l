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
		if mb, err := strconv.Atoi(env); err == nil && mb >= 200 && mb <= 1950 {
			return int64(mb) * 1024 * 1024
		}
	}
	// Default safe 1400 MiB slice: fits comfortably on 2GB disks with 600MB+ free space
	return 1400 * 1024 * 1024
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

type progressWriter struct {
	w          io.Writer
	onProgress func(n int64)
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 && pw.onProgress != nil {
		pw.onProgress(int64(n))
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

	_ = os.MkdirAll(DownDir, 0755)

	client := &http.Client{
		Timeout: 0, // No global timeout for large streaming transfers
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
	// Case 1: Single Part File (<= 1900 MiB)
	// -------------------------------------------------------------------
	if numParts == 1 {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf(
			"📥 **Starting Download (Go Engine)**\n\n"+
				"📁 `%s`\n"+
				"📦 **Size:** `%s`",
			filename, telegram.FormatBytes(totalSize),
		))

		localPath := filepath.Join(DownDir, filename)
		dlStart := time.Now()
		var downloaded int64 = 0

		f, err := os.Create(localPath)
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Disk error: %s", err))
			return
		}

		dlReq, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Request error: %s", err))
			return
		}
		dlReq.Header.Set("User-Agent", UserAgent)

		dlResp, err := client.Do(dlReq)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Download connection failed: %s", err))
			return
		}

		pw := &progressWriter{
			w: f,
			onProgress: func(n int64) {
				downloaded += n
				pText := telegram.RenderProgress("📥 **Downloading from URL**", filename, downloaded, totalSize, dlStart, "")
				updateProgress(pText)
			},
		}

		_, copyErr := io.Copy(pw, dlResp.Body)
		_ = dlResp.Body.Close()
		_ = f.Close()

		if copyErr != nil {
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Download interrupted: %s", copyErr))
			return
		}

		// Verify size <= 2000 MiB
		stat, _ := os.Stat(localPath)
		if stat != nil && stat.Size() > 2000*1024*1024 {
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, "❌ File exceeded Telegram 2000 MiB limit.")
			return
		}

		// Upload using primary bot
		primary := b.pool.Primary()
		if _, err := b.pool.Resolver().GetInputChannel(ctx, primary.API); err != nil {
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Storage channel error: %s", err))
			return
		}

		upStart := time.Now()
		tracker := &uploadProgressTracker{
			onProgress: func(uploaded int64) {
				pText := telegram.RenderProgress("📤 **Uploading to Telegram**", filename, uploaded, totalSize, upStart, "")
				updateProgress(pText)
			},
		}

		var sendRes tg.UpdatesClass
		for attempt := 1; attempt <= 4; attempt++ {
			worker := primary
			if attempt > 1 {
				worker = b.pool.NextWorker()
				time.Sleep(3 * time.Second)
			}
			u := uploader.NewUploader(worker.API).WithThreads(4).WithPartSize(512 * 1024).WithProgress(tracker)
			inputFile, upErr := u.FromPath(ctx, localPath)
			if upErr != nil || inputFile == nil {
				slog.Warn("Single file upload attempt failed", "attempt", attempt, "err", upErr)
				continue
			}

			wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, worker.API)
			if chErr != nil || wCh == nil {
				slog.Warn("Could not resolve storage channel for worker", "worker_id", worker.ID, "err", chErr)
				continue
			}

			mimeType := mime.TypeByExtension(filepath.Ext(filename))
			if mimeType == "" {
				mimeType = "video/mp4"
			}

			capText, capEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s`\n📦 `%s`", filename, telegram.FormatBytes(totalSize)))
			res, sendErr := worker.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
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
				slog.Warn("Single file send media attempt failed", "attempt", attempt, "err", sendErr)
				continue
			}
			sendRes = res
			break
		}
		_ = os.Remove(localPath)
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
			"🎬 **Leech Complete! (Go Fast-Engine)**\n\n"+
				"📁 **File Name:** `%s`\n"+
				"📦 **File Size:** `%s`\n"+
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
	// Case 2: Multi-Bot Parallel Pipelined Leech (> 2GB to 20GB+)
	// -------------------------------------------------------------------
	activeWorkers := numBots
	if numParts < activeWorkers {
		activeWorkers = numParts
	}

	modeDesc := "RFC 7233 Parallel Range Slicing"
	if !supportsRange {
		modeDesc = "Dynamic Pipelined Stream Slicing"
	}

	b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf(
		"🚀 **Multi-Bot Parallel Accelerator Engaged! (Go)**\n\n"+
			"📁 `%s` (%s)\n"+
			"🤖 **Worker Bots:** `%d Bots in pool`\n"+
			"🧩 **Slices:** `%d parts` (~%s each)\n"+
			"⚡ **Protocol:** `%s`",
		cleanName, telegram.FormatBytes(totalSize), activeWorkers, numParts, telegram.FormatBytes(sliceSize), modeDesc,
	))

	msgIDs := make([]int, numParts)
	partDownloaded := make([]atomic.Int64, numParts)
	partUploaded := make([]atomic.Int64, numParts)
	var (
		dlStart time.Time
		dlOnce  sync.Once
		upStart time.Time
		upOnce  sync.Once
	)

	primary := b.pool.Primary()
	storageChannel, err := b.pool.Resolver().GetInputChannel(ctx, primary.API)
	if err != nil {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Storage channel error: %s", err))
		return
	}

	if supportsRange {
		// Method A: Remote server supports RFC 7233 Range headers
		type partJob struct {
			index int
		}
		jobChan := make(chan partJob, numParts)
		for i := 0; i < numParts; i++ {
			jobChan <- partJob{index: i}
		}
		close(jobChan)

		// Utilize all available worker bots in parallel for maximum upload speed
		downloadSem := make(chan struct{}, activeWorkers)

		var wg sync.WaitGroup
		for wIdx := 0; wIdx < activeWorkers; wIdx++ {
			worker := availableWorkers[wIdx%numBots]
			wg.Add(1)

			go func(wClient *telegram.WorkerClient) {
				defer wg.Done()

				for job := range jobChan {
					partIdx := job.index
					partNum := partIdx + 1
					partStart := int64(partIdx) * sliceSize
					partEnd := int64(math.Min(float64(totalSize-1), float64(int64(partIdx+1)*sliceSize-1)))
					partLen := partEnd - partStart + 1
					partFilename := fmt.Sprintf("%s.%03d", cleanName, partNum)
					partPath := filepath.Join(DownDir, partFilename)

					func() {
						downloadSem <- struct{}{}
						defer func() { <-downloadSem }()
						defer os.Remove(partPath)

						slog.Info("Downloading slice", "part", partNum, "start", partStart, "end", partEnd, "len", partLen)

						// Download slice with Range (with up to 3 retries)
						var pResp *http.Response
						for dlAttempt := 1; dlAttempt <= 3; dlAttempt++ {
							pReq, _ := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
							pReq.Header.Set("User-Agent", UserAgent)
							pReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partStart, partEnd))

							resp, err := client.Do(pReq)
							if err == nil && (resp.StatusCode == http.StatusPartialContent || resp.StatusCode == http.StatusOK) {
								pResp = resp
								break
							}
							if resp != nil {
								_ = resp.Body.Close()
							}
							slog.Warn("Range download attempt failed", "part", partNum, "attempt", dlAttempt, "err", err)
							time.Sleep(2 * time.Second)
						}
						if pResp == nil {
							slog.Error("Range download failed completely after 3 attempts", "part", partNum)
							return
						}

						pf, err := os.Create(partPath)
						if err != nil {
							_ = pResp.Body.Close()
							slog.Error("Create part file failed", "part", partNum, "err", err)
							return
						}

						var currentPartDl int64
						pw := &progressWriter{
							w: pf,
							onProgress: func(n int64) {
								dlOnce.Do(func() { dlStart = time.Now() })
								currentPartDl += n
								partDownloaded[partIdx].Store(currentPartDl)
								var currDl int64
								for i := range partDownloaded {
									currDl += partDownloaded[i].Load()
								}
								pText := telegram.RenderProgress("📥 **Downloading Parts from URL**", cleanName, currDl, totalSize, dlStart,
									fmt.Sprintf("Part %d/%d | %s", partNum, numParts, modeDesc))
								updateProgress(pText)
							},
						}

						_, copyErr := io.CopyN(pw, pResp.Body, partLen)
						_ = pResp.Body.Close()
						_ = pf.Close()

						if copyErr != nil && copyErr != io.EOF {
							slog.Warn("Copy slice body error", "part", partNum, "err", copyErr)
						}

						slog.Info("Uploading slice to Telegram", "part", partNum, "path", partPath)

						// Upload to Telegram with accurate progress tracker
						partTracker := &uploadProgressTracker{
							onProgress: func(uploaded int64) {
								upOnce.Do(func() { upStart = time.Now() })
								partUploaded[partIdx].Store(uploaded)
								var currUp int64
								for i := range partUploaded {
									currUp += partUploaded[i].Load()
								}
								pText := telegram.RenderProgress("📤 **Multi-Bot Parallel Upload**", cleanName, currUp, totalSize, upStart,
									fmt.Sprintf("Part %d/%d | %d Bots active", partNum, numParts, activeWorkers))
								updateProgress(pText)
							},
						}

						for attempt := 1; attempt <= 4; attempt++ {
							worker := wClient
							if attempt > 1 {
								worker = b.pool.NextWorker()
								time.Sleep(3 * time.Second)
							}
							partUploaded[partIdx].Store(0)

							u := uploader.NewUploader(worker.API).WithThreads(4).WithPartSize(512 * 1024).WithProgress(partTracker)
							inputFile, uploadErr := u.FromPath(ctx, partPath)
							if uploadErr != nil || inputFile == nil {
								slog.Warn("Upload slice attempt failed", "part", partNum, "attempt", attempt, "err", uploadErr)
								continue
							}

							wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, worker.API)
							if chErr != nil || wCh == nil {
								slog.Warn("Could not resolve storage channel for worker", "part", partNum, "worker_id", worker.ID, "err", chErr)
								continue
							}

							partCap, partEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", partFilename, partNum, numParts, telegram.FormatBytes(partLen)))
							sRes, sendErr := worker.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
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
								slog.Warn("Send slice media attempt failed", "part", partNum, "attempt", attempt, "err", sendErr)
								continue
							}

							if msgID := extractMsgID(sRes); msgID > 0 {
								msgIDs[partIdx] = msgID
								slog.Info("Slice successfully saved in channel", "part", partNum, "msg_id", msgID)
								break
							}
						}
					}()
				}
			}(worker)
		}
		wg.Wait()

	} else {
		// Method B: Stream sequentially and slice on the fly into <= 1900 MiB parts
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

		var uploadWg sync.WaitGroup
		uploadSem := make(chan struct{}, activeWorkers)

		for partIdx := 0; partIdx < numParts; partIdx++ {
			partNum := partIdx + 1
			partStart := int64(partIdx) * sliceSize
			partEnd := int64(math.Min(float64(totalSize-1), float64(int64(partIdx+1)*sliceSize-1)))
			partLen := partEnd - partStart + 1
			partFilename := fmt.Sprintf("%s.%03d", cleanName, partNum)
			partPath := filepath.Join(DownDir, partFilename)

			pf, err := os.Create(partPath)
			if err != nil {
				break
			}

			var currentPartDl int64
			pw := &progressWriter{
				w: pf,
				onProgress: func(n int64) {
					dlOnce.Do(func() { dlStart = time.Now() })
					currentPartDl += n
					partDownloaded[partIdx].Store(currentPartDl)
					var currDl int64
					for i := range partDownloaded {
						currDl += partDownloaded[i].Load()
					}
					pText := telegram.RenderProgress("📥 **Downloading Stream (Go)**", cleanName, currDl, totalSize, dlStart,
						fmt.Sprintf("Part %d/%d", partNum, numParts))
					updateProgress(pText)
				},
			}

			n, _ := io.CopyN(pw, sResp.Body, partLen)
			pf.Close()

			if n == 0 && partIdx > 0 {
				_ = os.Remove(partPath)
				break
			}

			worker := availableWorkers[partIdx%numBots]
			uploadWg.Add(1)

			go func(idx int, pNum int, pPath, pName string, pLen int64, wClient *telegram.WorkerClient) {
				defer uploadWg.Done()
				uploadSem <- struct{}{}
				defer func() { <-uploadSem }()
				defer os.Remove(pPath)

				partTracker := &uploadProgressTracker{
					onProgress: func(uploaded int64) {
						upOnce.Do(func() { upStart = time.Now() })
						partUploaded[idx].Store(uploaded)
						var currUp int64
						for i := range partUploaded {
							currUp += partUploaded[i].Load()
						}
						pText := telegram.RenderProgress("📤 **Multi-Bot Parallel Upload**", cleanName, currUp, totalSize, upStart,
							fmt.Sprintf("Part %d/%d | %d Bots active", pNum, numParts, activeWorkers))
						updateProgress(pText)
					},
				}

				for attempt := 1; attempt <= 4; attempt++ {
					worker := wClient
					if attempt > 1 {
						worker = b.pool.NextWorker()
						time.Sleep(3 * time.Second)
					}
					partUploaded[idx].Store(0)

					u := uploader.NewUploader(worker.API).WithThreads(4).WithPartSize(512 * 1024).WithProgress(partTracker)
					inputFile, uploadErr := u.FromPath(ctx, pPath)
					if uploadErr != nil || inputFile == nil {
						slog.Warn("Upload stream slice attempt failed", "part", pNum, "attempt", attempt, "err", uploadErr)
						continue
					}

					wCh, chErr := b.pool.Resolver().GetInputChannel(ctx, worker.API)
					if chErr != nil || wCh == nil {
						slog.Warn("Could not resolve storage channel for worker", "part", pNum, "worker_id", worker.ID, "err", chErr)
						continue
					}

					pCap, pEntities := telegram.ParseMarkdown(fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", pName, pNum, numParts, telegram.FormatBytes(pLen)))
					sRes, sendErr := worker.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
						Peer: &tg.InputPeerChannel{
							ChannelID:  wCh.ChannelID,
							AccessHash: wCh.AccessHash,
						},
						Media: &tg.InputMediaUploadedDocument{
							File:     inputFile,
							MimeType: "video/x-matroska",
							Attributes: []tg.DocumentAttributeClass{
								&tg.DocumentAttributeFilename{FileName: pName},
							},
						},
						Message:  pCap,
						Entities: pEntities,
						RandomID: randInt64(),
					})
					if sendErr != nil || sRes == nil {
						slog.Warn("Send stream slice media attempt failed", "part", pNum, "attempt", attempt, "err", sendErr)
						continue
					}

					if msgID := extractMsgID(sRes); msgID > 0 {
						msgIDs[idx] = msgID
						slog.Info("Stream slice successfully saved in channel", "part", pNum, "msg_id", msgID)
						break
					}
				}
			}(partIdx, partNum, partPath, partFilename, n, worker)
		}

		uploadWg.Wait()
	}

	// Verify that all parts uploaded successfully
	var missingParts []int
	for i, id := range msgIDs {
		if id <= 0 {
			missingParts = append(missingParts, i+1)
		}
	}

	if len(missingParts) > 0 {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Leech aborted: Part(s) %v failed to upload after retries. Video must be 100%% complete to stream properly.", missingParts))
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
		_, editErr := primary.API.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
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
		sendRes, sErr := primary.API.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
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
		"🎬 **Multi-Bot Parallel Leech Complete! (Go Engine)**\n\n"+
			"📁 **Merged Name:** `%s`\n"+
			"📦 **Total Size:** `%s` (%d Parts)\n"+
			"🤖 **Speed Boost:** Uploaded with %d bots in parallel!\n"+
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
