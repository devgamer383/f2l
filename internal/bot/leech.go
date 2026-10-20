package bot

import (
	"context"
	"crypto/rand"
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"f2l/internal/stream"
	"f2l/internal/telegram"

	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
)

const (
	// Safe 1900 MiB slice size strictly below Telegram's 2000 MiB ceiling
	PartSize   int64 = 1900 * 1024 * 1024
	DownDir          = "downloads"
)

var cdRegex = regexp.MustCompile(`(?i)filename\*?=(?:UTF-8'')?["']?([^"';\r\n]+)`)

func randInt64() int64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return n.Int64()
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

	statusMsgID, err := b.sendReply(ctx, peer, replyToID, "⏳ Connecting to URL and inspecting stream...")
	if err != nil {
		slog.Error("Failed to send initial leech status message", "error", err)
		return
	}

	_ = os.MkdirAll(DownDir, 0755)

	client := &http.Client{
		Timeout: 0, // No global timeout for large streaming transfers
	}

	// 1. Inspect URL with HEAD or GET
	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Invalid URL: %s", err))
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Connection failed: %s", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Remote server error: HTTP %d", resp.StatusCode))
		return
	}

	totalSize := resp.ContentLength
	cd := resp.Header.Get("Content-Disposition")
	rawFilename := customName
	if rawFilename == "" {
		rawFilename = extractFilename(targetURL, cd)
	}
	filename := sanitizeFilename(rawFilename)

	if totalSize <= 0 {
		b.editStatus(ctx, peer, statusMsgID, "❌ Remote server did not report Content-Length.")
		return
	}

	// Probe if server supports HTTP Range requests
	supportsRange := false
	probeReq, _ := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	probeReq.Header.Set("Range", "bytes=0-0")
	if probeResp, pErr := client.Do(probeReq); pErr == nil {
		if probeResp.StatusCode == http.StatusPartialContent {
			supportsRange = true
		}
		probeResp.Body.Close()
	}

	availableWorkers := b.pool.GetAvailableWorkers()
	numBots := len(availableWorkers)
	if numBots == 0 {
		numBots = 1
	}

	var numParts int
	if totalSize <= PartSize {
		numParts = 1
	} else {
		numParts = int(math.Ceil(float64(totalSize) / float64(PartSize)))
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
		localPath := filepath.Join(DownDir, filename)
		dlStart := time.Now()
		var downloaded int64 = 0

		f, err := os.Create(localPath)
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Disk error: %s", err))
			return
		}

		buf := make([]byte, 1024*1024)
		for {
			n, rErr := resp.Body.Read(buf)
			if n > 0 {
				_, _ = f.Write(buf[:n])
				downloaded += int64(n)

				pText := telegram.RenderProgress("📥 **Downloading from URL**", filename, downloaded, totalSize, dlStart, "")
				updateProgress(pText)
			}
			if rErr != nil {
				break
			}
		}
		f.Close()

		// Verify size <= 2000 MiB
		stat, _ := os.Stat(localPath)
		if stat != nil && stat.Size() > 2000*1024*1024 {
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, "❌ File exceeded Telegram 2000 MiB limit.")
			return
		}

		// Upload using primary bot
		primary := b.pool.Primary()
		storageChannel, err := b.pool.Resolver().GetInputChannel(ctx, primary.API)
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Storage channel error: %s", err))
			return
		}

		u := uploader.NewUploader(primary.API).WithThreads(4).WithPartSize(512 * 1024)
		inputFile, err := u.FromPath(ctx, localPath)
		if err != nil {
			_ = os.Remove(localPath)
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Telegram upload failed: %s", err))
			return
		}
		_ = os.Remove(localPath)

		// Send to storage channel
		mimeType := mime.TypeByExtension(filepath.Ext(filename))
		if mimeType == "" {
			mimeType = "video/mp4"
		}

		sendRes, err := primary.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  storageChannel.ChannelID,
				AccessHash: storageChannel.AccessHash,
			},
			Media: &tg.InputMediaUploadedDocument{
				File:     inputFile,
				MimeType: mimeType,
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: filename},
				},
			},
			Message:  fmt.Sprintf("📁 `%s`\n📦 `%s`", filename, telegram.FormatBytes(totalSize)),
			RandomID: randInt64(),
		})
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Failed to send document: %s", err))
			return
		}

		sentMsgID := 0
		if updates, ok := sendRes.(*tg.Updates); ok {
			for _, upd := range updates.Updates {
				if nu, ok := upd.(*tg.UpdateNewChannelMessage); ok {
					if m, ok := nu.Message.(*tg.Message); ok {
						sentMsgID = m.ID
						break
					}
				}
			}
		}

		streamURL := fmt.Sprintf("%s/inline/%d/%s", baseURL, sentMsgID, filename)
		playerURL := fmt.Sprintf("%s/stream/%d/%s", baseURL, sentMsgID, filename)
		downloadURL := fmt.Sprintf("%s/dl/%d/%s", baseURL, sentMsgID, filename)

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

		b.editStatus(ctx, peer, statusMsgID, successText)
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
			"🤖 **Worker Bots:** `%d Bots uploading in parallel`\n"+
			"🧩 **Slices:** `%d parts` (~%s each)\n"+
			"⚡ **Protocol:** `%s`",
		cleanName, telegram.FormatBytes(totalSize), activeWorkers, numParts, telegram.FormatBytes(sliceSize), modeDesc,
	))

	msgIDs := make([]int, numParts)
	var totalUploaded atomic.Int64
	upStart := time.Now()

	updateAggregate := func() {
		current := totalUploaded.Load()
		pText := telegram.RenderProgress("📤 **Multi-Bot Parallel Upload (Go)**", cleanName, current, totalSize, upStart,
			fmt.Sprintf("%d Bots active | %d parts", activeWorkers, numParts))
		updateProgress(pText)
	}

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
						defer os.Remove(partPath)

						// Download slice with Range
						pReq, _ := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
						pReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partStart, partEnd))

						pResp, err := client.Do(pReq)
						if err != nil || pResp.StatusCode != http.StatusPartialContent {
							slog.Warn("Range request failed", "part", partNum, "err", err)
							return
						}
						defer pResp.Body.Close()

						pf, err := os.Create(partPath)
						if err != nil {
							return
						}

						// Read strictly partLen bytes
						_, _ = io.CopyN(pf, pResp.Body, partLen)
						pf.Close()

						// Upload to Telegram
						u := uploader.NewUploader(wClient.API).WithThreads(4).WithPartSize(512 * 1024)
						inputFile, err := u.FromPath(ctx, partPath)
						if err != nil {
							// Fallback to primary
							u = uploader.NewUploader(primary.API).WithThreads(4).WithPartSize(512 * 1024)
							inputFile, err = u.FromPath(ctx, partPath)
						}
						if err != nil {
							slog.Error("Slice upload failed", "part", partNum, "err", err)
							return
						}

						sRes, err := wClient.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
							Peer: &tg.InputPeerChannel{
								ChannelID:  storageChannel.ChannelID,
								AccessHash: storageChannel.AccessHash,
							},
							Media: &tg.InputMediaUploadedDocument{
								File:     inputFile,
								MimeType: "video/x-matroska",
								Attributes: []tg.DocumentAttributeClass{
									&tg.DocumentAttributeFilename{FileName: partFilename},
								},
							},
							Message:  fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", partFilename, partNum, numParts, telegram.FormatBytes(partLen)),
							RandomID: randInt64(),
						})
						if err != nil {
							slog.Error("Send slice media failed", "part", partNum, "err", err)
							return
						}

						if updates, ok := sRes.(*tg.Updates); ok {
							for _, upd := range updates.Updates {
								if nu, ok := upd.(*tg.UpdateNewChannelMessage); ok {
									if m, ok := nu.Message.(*tg.Message); ok {
										msgIDs[partIdx] = m.ID
										break
									}
								}
							}
						}

						totalUploaded.Add(partLen)
						updateAggregate()
					}()
				}
			}(worker)
		}
		wg.Wait()

	} else {
		// Method B: Stream sequentially and slice on the fly into <= 1900 MiB parts
		sResp, err := client.Get(targetURL)
		if err != nil {
			b.editStatus(ctx, peer, statusMsgID, fmt.Sprintf("❌ Stream connection failed: %s", err))
			return
		}
		defer sResp.Body.Close()

		var uploadWg sync.WaitGroup
		uploadSem := make(chan struct{}, numBots)

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

			// Read strictly partLen bytes into this slice file
			n, _ := io.CopyN(pf, sResp.Body, partLen)
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

				u := uploader.NewUploader(wClient.API).WithThreads(4).WithPartSize(512 * 1024)
				inputFile, err := u.FromPath(ctx, pPath)
				if err != nil {
					u = uploader.NewUploader(primary.API).WithThreads(4).WithPartSize(512 * 1024)
					inputFile, err = u.FromPath(ctx, pPath)
				}
				if err != nil {
					slog.Error("Upload stream slice failed", "part", pNum, "err", err)
					return
				}

				sRes, err := wClient.API.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
					Peer: &tg.InputPeerChannel{
						ChannelID:  storageChannel.ChannelID,
						AccessHash: storageChannel.AccessHash,
					},
					Media: &tg.InputMediaUploadedDocument{
						File:     inputFile,
						MimeType: "video/x-matroska",
						Attributes: []tg.DocumentAttributeClass{
							&tg.DocumentAttributeFilename{FileName: pName},
						},
					},
					Message:  fmt.Sprintf("📁 `%s` (Part %d/%d)\n📦 `%s`", pName, pNum, numParts, telegram.FormatBytes(pLen)),
					RandomID: randInt64(),
				})
				if err != nil {
					slog.Error("Send stream slice failed", "part", pNum, "err", err)
					return
				}

				if updates, ok := sRes.(*tg.Updates); ok {
					for _, upd := range updates.Updates {
						if nu, ok := upd.(*tg.UpdateNewChannelMessage); ok {
							if m, ok := nu.Message.(*tg.Message); ok {
								msgIDs[idx] = m.ID
								break
							}
						}
					}
				}

				totalUploaded.Add(pLen)
				updateAggregate()
			}(partIdx, partNum, partPath, partFilename, n, worker)
		}

		uploadWg.Wait()
	}

	// Filter successful message IDs
	var validIDs []int
	for _, id := range msgIDs {
		if id > 0 {
			validIDs = append(validIDs, id)
		}
	}

	if len(validIDs) == 0 {
		b.editStatus(ctx, peer, statusMsgID, "❌ All slice uploads failed.")
		return
	}

	batchToken := stream.EncodeBatch(validIDs)
	streamURL := fmt.Sprintf("%s/inline/v/%s/%s", baseURL, batchToken, cleanName)
	playerURL := fmt.Sprintf("%s/v/%s/%s", baseURL, batchToken, cleanName)
	downloadURL := fmt.Sprintf("%s/dl/v/%s/%s", baseURL, batchToken, cleanName)

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

	b.editStatus(ctx, peer, statusMsgID, successText)
}
