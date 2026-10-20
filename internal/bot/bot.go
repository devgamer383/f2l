package bot

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"f2l/internal/cache"
	"f2l/internal/config"
	"f2l/internal/security"
	"f2l/internal/stream"
	"f2l/internal/telegram"

	"github.com/gotd/td/tg"
)

type Bot struct {
	cfg           *config.Config
	pool          *telegram.BotPool
	virtualStream *stream.VirtualStreamEngine
	cache         *cache.LRUCache
	dispatcher    tg.UpdateDispatcher
}

func NewBot(cfg *config.Config, pool *telegram.BotPool, virtualStream *stream.VirtualStreamEngine, chunkCache *cache.LRUCache) *Bot {
	b := &Bot{
		cfg:           cfg,
		pool:          pool,
		virtualStream: virtualStream,
		cache:         chunkCache,
		dispatcher:    tg.NewUpdateDispatcher(),
	}

	b.registerHandlers()
	return b
}

func (b *Bot) Dispatcher() tg.UpdateDispatcher {
	return b.dispatcher
}

func (b *Bot) isUserAllowed(userID int64) bool {
	if b.cfg.Telegram.OwnerID != 0 && userID == b.cfg.Telegram.OwnerID {
		return true
	}
	if len(b.cfg.Telegram.AllowedUserIDs) == 0 {
		return true // public mode if not specified
	}
	for _, id := range b.cfg.Telegram.AllowedUserIDs {
		if id == userID {
			return true
		}
	}
	return false
}

func createStreamMarkup(streamURL, playerURL, downloadURL string) *tg.ReplyInlineMarkup {
	return &tg.ReplyInlineMarkup{
		Rows: []tg.KeyboardButtonRow{
			{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonURL{Text: "⚡ Direct Stream", URL: streamURL},
					&tg.KeyboardButtonURL{Text: "📥 Fast Download", URL: downloadURL},
				},
			},
			{
				Buttons: []tg.KeyboardButtonClass{
					&tg.KeyboardButtonURL{Text: "🎬 Open in VLC / Web Player", URL: playerURL},
				},
			},
		},
	}
}

func (b *Bot) registerHandlers() {
	b.dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		msg, ok := u.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}

		peerUser, ok := msg.PeerID.(*tg.PeerUser)
		if !ok {
			return nil // only handle private user chats
		}

		var userPeer tg.InputPeerClass
		firstName := "User"
		if user, ok := e.Users[peerUser.UserID]; ok {
			userPeer = user.AsInputPeer()
			if user.FirstName != "" {
				firstName = user.FirstName
			}
		} else {
			userPeer = &tg.InputPeerUser{
				UserID: peerUser.UserID,
			}
		}

		if !b.isUserAllowed(peerUser.UserID) {
			b.replyText(ctx, userPeer, msg.ID, "⛔ **Access Denied:** You are not authorized to use this bot.")
			return nil
		}

		text := strings.TrimSpace(msg.Message)
		slog.Info("📩 Received private message", "user_id", peerUser.UserID, "text", text, "has_media", msg.Media != nil)

		// 1. Check commands
		if strings.HasPrefix(text, "/") {
			parts := strings.Fields(text)
			cmd := strings.ToLower(parts[0])
			cmd = strings.TrimSuffix(cmd, "@"+strings.ToLower(b.cfg.Telegram.BotUsername))

			switch cmd {
			case "/start":
				b.handleStart(ctx, userPeer, msg.ID, firstName)
				return nil
			case "/help":
				b.handleHelp(ctx, userPeer, msg.ID)
				return nil
			case "/status":
				b.handleStatus(ctx, userPeer, msg.ID)
				return nil
			case "/privacy":
				b.handlePrivacy(ctx, userPeer, msg.ID)
				return nil
			case "/merge", "/stitch", "/join":
				replyTargetID := 0
				if rh, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
					replyTargetID = rh.ReplyToMsgID
				}
				b.handleMerge(ctx, userPeer, msg.ID, replyTargetID, parts[1:])
				return nil
			case "/leech", "/download", "/fetch":
				go b.handleLeech(context.Background(), userPeer, msg.ID, parts[1:])
				return nil
			}
		}

		// 2. Check incoming media files (user sent or forwarded a file)
		if msg.Media != nil {
			go b.handleIncomingMedia(ctx, userPeer, msg)
			return nil
		}

		return nil
	})
}

func (b *Bot) handleStart(ctx context.Context, peer tg.InputPeerClass, replyToID int, firstName string) {
	text := fmt.Sprintf(
		"👋 **Hello, %s!**\n\n"+
			"🚀 **High-Speed Telegram Stream & Direct Link Bot (Go Edition)**\n\n"+
			"⚡ **Features:**\n"+
			"• Instant streaming & download links for any file\n"+
			"• Ultra-low latency RFC 7233 byte-range streaming for VLC & Web Players\n"+
			"• 10GB+ Virtual file stitching with `/merge`\n"+
			"• High-speed multi-bot parallel leeching with `/leech`\n"+
			"• In-memory LRU caching with 0ms seek response\n\n"+
			"📤 **Send or forward me any video, document, or audio file to get started!**",
		firstName,
	)
	b.replyText(ctx, peer, replyToID, text)
}

func (b *Bot) handleHelp(ctx context.Context, peer tg.InputPeerClass, replyToID int) {
	text := "ℹ️ **Available Commands:**\n\n" +
		"• `/start` - Check if the bot is alive & show welcome card\n" +
		"• `/help` - Show this command reference\n" +
		"• `/status` - View live streaming engine performance & cache stats\n" +
		"• `/privacy` - View privacy policy\n" +
		"• `/leech <url> [name.mkv]` - Leech any direct link (splits >2GB into virtual parts)\n" +
		"• `/merge <count>` - Reply to part 1 to merge split 2GB parts into 1 seamless stream\n\n" +
		"💡 _Just send or forward any video or document to get instant direct links!_"
	b.replyText(ctx, peer, replyToID, text)
}

func (b *Bot) handlePrivacy(ctx context.Context, peer tg.InputPeerClass, replyToID int) {
	text := "🔒 **Privacy Policy**\n\n" +
		"• This bot stores files strictly in your configured private storage channel.\n" +
		"• All streaming links are stateless and cryptographically signed.\n" +
		"• No personal data, chat history, or logs are shared with third parties."
	b.replyText(ctx, peer, replyToID, text)
}

func (b *Bot) handleStatus(ctx context.Context, peer tg.InputPeerClass, replyToID int) {
	stats := b.cache.GetStats()
	workerCount := len(b.pool.GetAvailableWorkers())

	text := fmt.Sprintf(
		"📊 **FileStream Performance Dashboard (Go Edition)**\n\n"+
			"⚡ **Stream Engine:** Parallel Prefetch (%dx concurrency)\n"+
			"🤖 **Active Worker Clients:** `%d`\n"+
			"💾 **LRU Chunk Cache:** `%.1f MB / %.1f MB`\n"+
			"📦 **Cached Chunks:** `%d`\n"+
			"🎯 **Cache Hit Ratio:** `%.1f%%` (`%d` hits, `%d` misses)\n"+
			"🌐 **Server Port:** `%d`",
		b.cfg.Stream.PrefetchChunks,
		workerCount,
		stats.CachedSizeMB,
		stats.MaxSizeMB,
		stats.CachedChunks,
		stats.HitRatio,
		stats.Hits,
		stats.Misses,
		b.cfg.Server.Port,
	)
	b.replyText(ctx, peer, replyToID, text)
}

func (b *Bot) handleIncomingMedia(ctx context.Context, peer tg.InputPeerClass, msg *tg.Message) {
	props, err := telegram.GetFileProperties(msg)
	if err != nil {
		return // Unsupported media type
	}

	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return
	}

	storageChannel, err := b.pool.Resolver().GetInputChannel(ctx, primary.API)
	if err != nil {
		slog.Error("Storage channel error", "error", err)
		return
	}

	// Forward / copy message to storage channel
	fwdRes, err := primary.API.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer: peer,
		ToPeer: &tg.InputPeerChannel{
			ChannelID:  storageChannel.ChannelID,
			AccessHash: storageChannel.AccessHash,
		},
		ID:       []int{msg.ID},
		RandomID: []int64{randInt64()},
	})
	if err != nil {
		slog.Error("Forward to storage channel failed", "error", err)
		return
	}

	channelMsgID := extractMsgID(fwdRes)
	if channelMsgID == 0 {
		slog.Error("Could not resolve forwarded message ID in storage channel", "res_type", fmt.Sprintf("%T", fwdRes))
		return
	}

	baseURL := strings.TrimRight(b.cfg.Server.BaseURL, "/")
	slug := security.EncodeFileSlug(channelMsgID, b.cfg.Telegram.BotToken)
	cleanName := telegram.CleanURLFilename(props.Name)
	streamURL := fmt.Sprintf("%s/inline/%s/%s", baseURL, slug, cleanName)
	playerURL := fmt.Sprintf("%s/watch/%s/%s?v=v2", baseURL, slug, cleanName)
	downloadURL := fmt.Sprintf("%s/dl/%s/%s", baseURL, slug, cleanName)

	reply := fmt.Sprintf(
		"📁 **File Name:** `%s`\n"+
			"📦 **File Size:** `%s`\n"+
			"⚡ **Stream Protocol:** `RFC 7233 (Zero-Copy Go)`\n\n"+
			"🔗 **Direct Links:**\n"+
			"• **Direct Stream:** [Click Here](%s)\n"+
			"• **Web & VLC Player:** [Click Here](%s)\n"+
			"• **Fast Download:** [Click Here](%s)",
		props.Name, telegram.FormatBytes(props.Size), streamURL, playerURL, downloadURL,
	)

	markup := createStreamMarkup(streamURL, playerURL, downloadURL)
	b.replyWithMarkup(ctx, peer, msg.ID, reply, markup)
}

func (b *Bot) handleMerge(ctx context.Context, peer tg.InputPeerClass, replyToID int, targetMsgID int, args []string) {
	if len(args) == 0 {
		b.replyText(ctx, peer, replyToID, "❌ **Usage:** Reply to part 1 with `/merge <total_parts>` (e.g. `/merge 5`)")
		return
	}

	count, err := strconv.Atoi(args[0])
	if err != nil || count < 2 || count > 50 {
		b.replyText(ctx, peer, replyToID, "❌ Count must be a valid number between 2 and 50.")
		return
	}

	if targetMsgID == 0 {
		b.replyText(ctx, peer, replyToID, "❌ Please reply to the first file part.")
		return
	}

	primary := b.pool.Primary()
	storageChannel, err := b.pool.Resolver().GetInputChannel(ctx, primary.API)
	if err != nil {
		b.replyText(ctx, peer, replyToID, "❌ Storage channel error.")
		return
	}

	// Forward replied message to storage channel to get channel message ID
	fwdRes, err := primary.API.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer: peer,
		ToPeer: &tg.InputPeerChannel{
			ChannelID:  storageChannel.ChannelID,
			AccessHash: storageChannel.AccessHash,
		},
		ID:       []int{targetMsgID},
		RandomID: []int64{randInt64()},
	})
	if err != nil {
		b.replyText(ctx, peer, replyToID, "❌ Could not process replied message.")
		return
	}

	startID := extractMsgID(fwdRes)
	if startID == 0 {
		b.replyText(ctx, peer, replyToID, "❌ Could not resolve start message ID in channel.")
		return
	}

	// Verify all parts exist
	msgIDs := make([]int, count)
	var inputMsgs []tg.InputMessageClass
	for i := 0; i < count; i++ {
		msgIDs[i] = startID + i
		inputMsgs = append(inputMsgs, &tg.InputMessageID{ID: startID + i})
	}

	res, err := primary.API.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: storageChannel,
		ID:      inputMsgs,
	})
	if err != nil {
		b.replyText(ctx, peer, replyToID, fmt.Sprintf("❌ Error verifying parts: %s", err))
		return
	}

	modified, ok := res.AsModified()
	if !ok {
		b.replyText(ctx, peer, replyToID, "❌ Could not retrieve messages from storage channel.")
		return
	}

	messages := modified.GetMessages()
	if len(messages) < count {
		b.replyText(ctx, peer, replyToID, "❌ Some parts were missing in the storage channel.")
		return
	}

	var totalSize int64 = 0
	unifiedName := ""

	for idx, m := range messages {
		msg, ok := m.(*tg.Message)
		if !ok {
			b.replyText(ctx, peer, replyToID, fmt.Sprintf("❌ Part %d is not a valid message.", idx+1))
			return
		}

		props, err := telegram.GetFileProperties(msg)
		if err != nil {
			b.replyText(ctx, peer, replyToID, fmt.Sprintf("❌ Part %d has unsupported media.", idx+1))
			return
		}

		totalSize += props.Size
		if idx == 0 {
			unifiedName = stream.CleanVirtualName(props.Name)
			if !strings.HasSuffix(unifiedName, ".mkv") && !strings.HasSuffix(unifiedName, ".mp4") {
				unifiedName += ".mkv"
			}
		}
	}

	baseURL := strings.TrimRight(b.cfg.Server.BaseURL, "/")
	
	// Hybrid Design: update Part 1 caption with timeline tag
	var batchToken string
	part1ID := msgIDs[0]
	joinedIDs := joinInts(msgIDs)

	if len(msgIDs) <= 100 {
		timelineTag := fmt.Sprintf("[timeline:%s]", joinedIDs)
		newCap := fmt.Sprintf("📁 `%s`\n📦 `%s` (%d Parts)\n🎬 `%s`", unifiedName, telegram.FormatBytes(totalSize), len(msgIDs), timelineTag)
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
		} else {
			batchToken = stream.EncodeBatch(msgIDs)
		}
	} else {
		batchToken = stream.EncodeBatch(msgIDs)
	}

	slug := security.EncodeVirtualSlug(batchToken, b.cfg.Telegram.BotToken)
	cleanUnified := telegram.CleanURLFilename(unifiedName)
	streamURL := fmt.Sprintf("%s/inline/v/%s/%s", baseURL, slug, cleanUnified)
	playerURL := fmt.Sprintf("%s/v/%s/%s?v=v2", baseURL, slug, cleanUnified)
	downloadURL := fmt.Sprintf("%s/dl/v/%s/%s", baseURL, slug, cleanUnified)

	reply := fmt.Sprintf(
		"🎬 **Virtual Merge Complete!**\n\n"+
			"📁 **Merged Name:** `%s`\n"+
			"📦 **Total Size:** `%s` (%d Parts)\n"+
			"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching`\n\n"+
			"🔗 **Single Seamless Links:**\n"+
			"• **Direct Stream:** [Click Here](%s)\n"+
			"• **Web & VLC Player:** [Click Here](%s)\n"+
			"• **Single Download:** [Click Here](%s)\n\n"+
			"💡 _All %d parts will stream and seek seamlessly as one contiguous file!_",
		unifiedName, telegram.FormatBytes(totalSize), count, streamURL, playerURL, downloadURL, count,
	)

	markup := createStreamMarkup(streamURL, playerURL, downloadURL)
	b.replyWithMarkup(ctx, peer, replyToID, reply, markup)
}

func (b *Bot) replyText(ctx context.Context, peer tg.InputPeerClass, replyToID int, text string) {
	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return
	}
	cleanMsg, entities := telegram.ParseMarkdown(text)
	_, _ = primary.API.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  cleanMsg,
		Entities: entities,
		ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: replyToID},
		RandomID: randInt64(),
	})
}

func (b *Bot) replyWithMarkup(ctx context.Context, peer tg.InputPeerClass, replyToID int, text string, markup tg.ReplyMarkupClass) {
	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return
	}
	cleanMsg, entities := telegram.ParseMarkdown(text)
	_, err := primary.API.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:        peer,
		Message:     cleanMsg,
		Entities:    entities,
		ReplyTo:     &tg.InputReplyToMessage{ReplyToMsgID: replyToID},
		RandomID:    randInt64(),
		ReplyMarkup: markup,
	})
	if err != nil {
		slog.Warn("Failed to send message with markup, falling back to clean text", "error", err)
		_, errFallback := primary.API.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peer,
			Message:  cleanMsg,
			Entities: entities,
			ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: replyToID},
			RandomID: randInt64(),
		})
		if errFallback != nil {
			slog.Error("Failed to send fallback message", "error", errFallback)
		}
	}
}

func (b *Bot) sendReply(ctx context.Context, peer tg.InputPeerClass, replyToID int, text string) (int, error) {
	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() {
		return 0, fmt.Errorf("bot not ready")
	}
	cleanMsg, entities := telegram.ParseMarkdown(text)
	res, err := primary.API.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  cleanMsg,
		Entities: entities,
		ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: replyToID},
		RandomID: randInt64(),
	})
	if err != nil {
		return 0, err
	}
	msgID := extractMsgID(res)
	if msgID == 0 {
		slog.Warn("Failed to extract message ID from sendReply result", "res_type", fmt.Sprintf("%T", res))
	}
	return msgID, nil
}

func (b *Bot) editStatus(ctx context.Context, peer tg.InputPeerClass, msgID int, text string) {
	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() || msgID == 0 {
		return
	}
	cleanMsg, entities := telegram.ParseMarkdown(text)
	_, err := primary.API.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
		Peer:     peer,
		ID:       msgID,
		Message:  cleanMsg,
		Entities: entities,
	})
	if err != nil {
		slog.Warn("Failed to edit status message", "msgID", msgID, "error", err)
	}
}

func (b *Bot) editStatusWithMarkup(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, markup tg.ReplyMarkupClass) {
	primary := b.pool.Primary()
	if primary == nil || !primary.IsReady.Load() || msgID == 0 {
		return
	}
	cleanMsg, entities := telegram.ParseMarkdown(text)
	_, err := primary.API.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
		Peer:        peer,
		ID:          msgID,
		Message:     cleanMsg,
		Entities:    entities,
		ReplyMarkup: markup,
	})
	if err != nil {
		slog.Warn("Failed to edit message with markup, falling back to clean text", "error", err)
		_, editErr := primary.API.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
			Peer:     peer,
			ID:       msgID,
			Message:  cleanMsg,
			Entities: entities,
		})
		if editErr != nil {
			slog.Warn("Failed to edit message fallback clean text", "msgID", msgID, "error", editErr)
		}
	}
}

func extractUpdateMsgID(u tg.UpdateClass) int {
	switch upd := u.(type) {
	case *tg.UpdateNewMessage:
		if upd.Message != nil {
			return upd.Message.GetID()
		}
	case *tg.UpdateNewChannelMessage:
		if upd.Message != nil {
			return upd.Message.GetID()
		}
	}
	return 0
}

func extractMsgID(res tg.UpdatesClass) int {
	if res == nil {
		return 0
	}
	switch u := res.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID
	case *tg.UpdateShortChatMessage:
		return u.ID
	case *tg.UpdateShortMessage:
		return u.ID
	case *tg.UpdateShort:
		return extractUpdateMsgID(u.Update)
	case *tg.Updates:
		for _, upd := range u.Updates {
			if id := extractUpdateMsgID(upd); id > 0 {
				return id
			}
		}
	case *tg.UpdatesCombined:
		for _, upd := range u.Updates {
			if id := extractUpdateMsgID(upd); id > 0 {
				return id
			}
		}
	}
	return 0
}
