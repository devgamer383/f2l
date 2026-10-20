package stream

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"f2l/internal/telegram"

	"github.com/gotd/td/tg"
)

var (
	partRegex       = regexp.MustCompile(`(?i)(\.00[1-9]|\.0[1-9][0-9]|\.part0*[1-9]|\.z0[1-9])+$`)
	sanitizerRegex  = regexp.MustCompile(`[\\/*?:"<>|]`)
	consecBatchRe   = regexp.MustCompile(`^s(\d+)c(\d+)$`)
	rangeBatchRe    = regexp.MustCompile(`^(\d+)-(\d+)$`)
)

func CleanVirtualName(raw string) string {
	cleaned := partRegex.ReplaceAllString(raw, "")
	cleaned = sanitizerRegex.ReplaceAllString(cleaned, "_")
	cleaned = strings.ReplaceAll(cleaned, " ", "-")
	if cleaned == "" {
		return raw
	}
	return cleaned
}

func EncodeBatch(msgIDs []int) string {
	if len(msgIDs) == 0 {
		return ""
	}
	// If consecutive, encode as s{start}c{count} e.g. s1050c5
	isConsecutive := true
	for i := 1; i < len(msgIDs); i++ {
		if msgIDs[i] != msgIDs[i-1]+1 {
			isConsecutive = false
			break
		}
	}
	if len(msgIDs) > 1 && isConsecutive {
		return fmt.Sprintf("s%dc%d", msgIDs[0], len(msgIDs))
	}

	var parts []string
	for _, id := range msgIDs {
		parts = append(parts, strconv.Itoa(id))
	}
	return strings.Join(parts, "_")
}

func DecodeBatch(batchToken string) ([]int, error) {
	if m := consecBatchRe.FindStringSubmatch(batchToken); len(m) == 3 {
		start, _ := strconv.Atoi(m[1])
		count, _ := strconv.Atoi(m[2])
		ids := make([]int, count)
		for i := 0; i < count; i++ {
			ids[i] = start + i
		}
		return ids, nil
	}

	if m := rangeBatchRe.FindStringSubmatch(batchToken); len(m) == 3 {
		start, _ := strconv.Atoi(m[1])
		end, _ := strconv.Atoi(m[2])
		if end >= start {
			count := end - start + 1
			ids := make([]int, count)
			for i := 0; i < count; i++ {
				ids[i] = start + i
			}
			return ids, nil
		}
	}

	parts := strings.Split(batchToken, "_")
	var ids []int
	for _, p := range parts {
		if id, err := strconv.Atoi(p); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) > 0 {
		return ids, nil
	}

	return nil, fmt.Errorf("invalid batch token: %s", batchToken)
}

type VirtualPart struct {
	MessageID int
	Size      int64
	Name      string
	Props     *telegram.FileProperties
}

type VirtualMetadata struct {
	BatchID   string
	Filename  string
	TotalSize int64
	MimeType  string
	Parts     []VirtualPart
}

type VirtualStreamEngine struct {
	fastStreamer *FastStreamEngine
	pool         *telegram.BotPool
	metaCache    map[string]*VirtualMetadata
	metaMu       sync.RWMutex
}

func NewVirtualStreamEngine(fastStreamer *FastStreamEngine, pool *telegram.BotPool) *VirtualStreamEngine {
	return &VirtualStreamEngine{
		fastStreamer: fastStreamer,
		pool:         pool,
		metaCache:    make(map[string]*VirtualMetadata),
	}
}

func (v *VirtualStreamEngine) GetVirtualMetadata(ctx context.Context, batchToken string) (*VirtualMetadata, error) {
	v.metaMu.RLock()
	if meta, found := v.metaCache[batchToken]; found {
		v.metaMu.RUnlock()
		return meta, nil
	}
	v.metaMu.RUnlock()

	msgIDs, err := DecodeBatch(batchToken)
	if err != nil {
		return nil, err
	}

	primary := v.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return nil, fmt.Errorf("telegram client is not ready")
	}

	ch, err := v.pool.Resolver().GetInputChannel(ctx, primary.API)
	if err != nil {
		return nil, fmt.Errorf("resolve storage channel: %w", err)
	}

	var inputMsgs []tg.InputMessageClass
	for _, id := range msgIDs {
		inputMsgs = append(inputMsgs, &tg.InputMessageID{ID: id})
	}

	res, err := primary.API.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID:      inputMsgs,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch batch messages: %w", err)
	}

	modified, ok := res.AsModified()
	if !ok {
		return nil, fmt.Errorf("fetch batch messages returned unmodified")
	}

	msgMap := make(map[int]*tg.Message)
	for _, m := range modified.GetMessages() {
		if msg, ok := m.(*tg.Message); ok {
			msgMap[msg.ID] = msg
		}
	}

	var parts []VirtualPart
	var totalSize int64 = 0
	unifiedName := ""
	mimeType := "video/x-matroska"

	for idx, id := range msgIDs {
		msg, ok := msgMap[id]
		if !ok {
			return nil, fmt.Errorf("message ID %d not found in storage channel", id)
		}

		props, err := telegram.GetFileProperties(msg)
		if err != nil {
			return nil, fmt.Errorf("message ID %d: %w", id, err)
		}

		if idx == 0 {
			unifiedName = CleanVirtualName(props.Name)
			mimeType = props.MimeType
		}

		parts = append(parts, VirtualPart{
			MessageID: id,
			Size:      props.Size,
			Name:      props.Name,
			Props:     props,
		})
		totalSize += props.Size
	}

	meta := &VirtualMetadata{
		BatchID:   batchToken,
		Filename:  unifiedName,
		TotalSize: totalSize,
		MimeType:  mimeType,
		Parts:     parts,
	}

	v.metaMu.Lock()
	if len(v.metaCache) > 100 {
		for k := range v.metaCache {
			delete(v.metaCache, k)
			break
		}
	}
	v.metaCache[batchToken] = meta
	v.metaMu.Unlock()

	return meta, nil
}

func (v *VirtualStreamEngine) StreamVirtualRange(
	ctx context.Context,
	w http.ResponseWriter,
	batchID string,
	startByte, endByte, contentLength int64,
) error {
	meta, err := v.GetVirtualMetadata(ctx, batchID)
	if err != nil {
		return err
	}

	var partGlobalStart int64 = 0
	var bytesStreamed int64 = 0

	for _, part := range meta.Parts {
		partSize := part.Size
		partGlobalEnd := partGlobalStart + partSize - 1

		overlapStart := startByte
		if partGlobalStart > overlapStart {
			overlapStart = partGlobalStart
		}

		overlapEnd := endByte
		if partGlobalEnd < overlapEnd {
			overlapEnd = partGlobalEnd
		}

		if overlapStart <= overlapEnd {
			localStart := overlapStart - partGlobalStart
			localEnd := overlapEnd - partGlobalStart
			sliceLen := localEnd - localStart + 1

			err := v.fastStreamer.StreamFileRange(
				ctx,
				w,
				part.MessageID,
				localStart,
				localEnd,
				sliceLen,
				part.Props,
			)
			if err != nil {
				return err
			}

			bytesStreamed += sliceLen
			if bytesStreamed >= contentLength {
				break
			}
		}

		partGlobalStart += partSize
	}

	return nil
}
