package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Telegram TelegramConfig
	Server   ServerConfig
	Stream   StreamConfig
}

type TelegramConfig struct {
	APIID              int
	APIHash            string
	BotToken           string
	BotUsername        string
	ChannelID          int64
	OwnerID            int64
	AllowedUserIDs     []int64
	SecondaryBotTokens []string
}

type ServerConfig struct {
	Port        int
	BindAddress string
	BaseURL     string
	Version     string
}

type StreamConfig struct {
	ChunkSize      int
	PrefetchChunks int
	CacheSizeMB    int
	CacheTTLSec    int
}

func getEnvStr(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func Load() *Config {
	apiID := getEnvInt("TELEGRAM_API_ID", 0)
	if apiID == 0 {
		apiID = getEnvInt("API_ID", 0)
	}

	apiHash := getEnvStr("TELEGRAM_API_HASH", "")
	if apiHash == "" {
		apiHash = getEnvStr("API_HASH", "")
	}

	botToken := getEnvStr("TELEGRAM_BOT_TOKEN", "")
	if botToken == "" {
		botToken = getEnvStr("BOT_TOKEN", "")
	}

	botUsername := getEnvStr("TELEGRAM_BOT_USERNAME", "")
	if botUsername == "" {
		botUsername = getEnvStr("BOT_USERNAME", "")
	}

	channelID := getEnvInt64("TELEGRAM_CHANNEL_ID", 0)
	if channelID == 0 {
		channelID = getEnvInt64("CHANNEL_ID", 0)
	}

	ownerID := getEnvInt64("OWNER_ID", 0)

	// Secondary bot tokens
	rawMulti := getEnvStr("MULTI_BOT_TOKENS", "")
	var secondaryTokens []string
	if rawMulti != "" {
		fields := strings.FieldsFunc(rawMulti, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\n' || r == '\t'
		})
		for _, f := range fields {
			if trimmed := strings.TrimSpace(f); trimmed != "" {
				secondaryTokens = append(secondaryTokens, trimmed)
			}
		}
	}

	// Allowed user IDs
	rawAllowed := getEnvStr("ALLOWED_USER_IDS", "")
	var allowedUsers []int64
	if rawAllowed != "" {
		fields := strings.FieldsFunc(rawAllowed, func(r rune) bool {
			return r == ',' || r == ' '
		})
		for _, f := range fields {
			if id, err := strconv.ParseInt(strings.TrimSpace(f), 10, 64); err == nil {
				allowedUsers = append(allowedUsers, id)
			}
		}
	}

	baseURL := strings.TrimRight(getEnvStr("BASE_URL", ""), "/")
	if baseURL == "" {
		if appName := os.Getenv("HEROKU_APP_NAME"); appName != "" {
			baseURL = "https://" + appName + ".herokuapp.com"
		}
	}

	port := getEnvInt("PORT", 8080)

	return &Config{
		Telegram: TelegramConfig{
			APIID:              apiID,
			APIHash:            apiHash,
			BotToken:           botToken,
			BotUsername:        botUsername,
			ChannelID:          channelID,
			OwnerID:            ownerID,
			AllowedUserIDs:     allowedUsers,
			SecondaryBotTokens: secondaryTokens,
		},
		Server: ServerConfig{
			Port:        port,
			BindAddress: getEnvStr("BIND_ADDRESS", "0.0.0.0"),
			BaseURL:     baseURL,
			Version:     getEnvStr("VERSION", "2.1-go"),
		},
		Stream: StreamConfig{
			ChunkSize:      1024 * 1024, // 1 MB (Telegram MTProto hard limit)
			PrefetchChunks: getEnvInt("PREFETCH_CHUNKS", 8),
			CacheSizeMB:    getEnvInt("CACHE_SIZE_MB", 256),
			CacheTTLSec:    getEnvInt("CACHE_TTL_SEC", 1800),
		},
	}
}
