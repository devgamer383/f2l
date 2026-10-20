package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/gotd/td/tg"
)

func ParseChannelID(rawID int64) int64 {
	str := strconv.FormatInt(rawID, 10)
	if strings.HasPrefix(str, "-100") {
		str = strings.TrimPrefix(str, "-100")
		if parsed, err := strconv.ParseInt(str, 10, 64); err == nil {
			return parsed
		}
	}
	if rawID < 0 {
		return -rawID
	}
	return rawID
}

type ChannelResolver struct {
	mu           sync.RWMutex
	rawID        int64
	channelID    int64
	inputChannel *tg.InputChannel
}

func NewChannelResolver(channelID int64) *ChannelResolver {
	return &ChannelResolver{
		rawID:     channelID,
		channelID: ParseChannelID(channelID),
	}
}

func (cr *ChannelResolver) GetInputChannel(ctx context.Context, api *tg.Client) (*tg.InputChannel, error) {
	cr.mu.RLock()
	if cr.inputChannel != nil {
		ch := cr.inputChannel
		cr.mu.RUnlock()
		return ch, nil
	}
	cr.mu.RUnlock()

	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.inputChannel != nil {
		return cr.inputChannel, nil
	}

	slog.Info("Resolving storage channel access hash...", "channel_id", cr.channelID)

	// In Telegram MTProto, calling channels.getChannels with access_hash=0 resolves the channel
	res, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{
			ChannelID:  cr.channelID,
			AccessHash: 0,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get storage channel %d: %w", cr.channelID, err)
	}

	chats := res.GetChats()
	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok && ch.ID == cr.channelID {
			cr.inputChannel = &tg.InputChannel{
				ChannelID:  ch.ID,
				AccessHash: ch.AccessHash,
			}
			slog.Info("✅ Storage channel resolved successfully", "title", ch.Title, "channel_id", ch.ID)
			return cr.inputChannel, nil
		}
	}

	return nil, fmt.Errorf("channel %d not found in Telegram response", cr.channelID)
}

func (cr *ChannelResolver) ChannelID() int64 {
	return cr.channelID
}
