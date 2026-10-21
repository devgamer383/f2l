package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"f2l/internal/cache"
	"f2l/internal/config"
	"f2l/internal/security"
	"f2l/internal/stream"
	"f2l/internal/telegram"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

var rangeHeaderRe = regexp.MustCompile(`^bytes=(\d*)-(\d*)$`)

type Server struct {
	cfg            *config.Config
	cache          *cache.LRUCache
	fastStreamer   *stream.FastStreamEngine
	virtualStream  *stream.VirtualStreamEngine
	router         *chi.Mux
}

func NewServer(
	cfg *config.Config,
	chunkCache *cache.LRUCache,
	fastStreamer *stream.FastStreamEngine,
	virtualStream *stream.VirtualStreamEngine,
) *Server {
	s := &Server{
		cfg:           cfg,
		cache:         chunkCache,
		fastStreamer:  fastStreamer,
		virtualStream: virtualStream,
		router:        chi.NewRouter(),
	}

	s.setupRoutes()
	return s
}

func (s *Server) Router() http.Handler {
	return s.router
}

func (s *Server) getBaseURL(r *http.Request) string {
	if s.cfg.Server.BaseURL != "" {
		return strings.TrimRight(s.cfg.Server.BaseURL, "/")
	}
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

func (s *Server) setupRoutes() {
	r := s.router

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(corsMiddleware)

	// Health Check / Ping
	r.Get("/health", s.handleHealth)
	r.Get("/ping", s.handleHealth)

	// Home redirect to bot
	r.Get("/", s.handleHome)

	// Single-File Endpoints
	r.Get("/info/{file_id}", s.handleFileInfo)
	r.Method("HEAD", "/dl/{file_id}/{filename}", http.HandlerFunc(s.handleMediaTransmission(false)))
	r.Get("/dl/{file_id}/{filename}", s.handleMediaTransmission(false))

	r.Method("HEAD", "/inline/{file_id}/{filename}", http.HandlerFunc(s.handleMediaTransmission(true)))
	r.Get("/inline/{file_id}/{filename}", s.handleMediaTransmission(true))
	r.Method("HEAD", "/play/{file_id}/{filename}", http.HandlerFunc(s.handleMediaTransmission(true)))
	r.Get("/play/{file_id}/{filename}", s.handleMediaTransmission(true))

	r.Get("/stream/{file_id}/{filename}", s.handleWebPlayer)
	r.Get("/watch/{file_id}/{filename}", s.handleWebPlayer)

	// Virtual Multi-Part 10GB+ Endpoints
	r.Get("/info/v/{batch_id}", s.handleVirtualFileInfo)
	r.Method("HEAD", "/dl/v/{batch_id}/{filename}", http.HandlerFunc(s.handleVirtualTransmission(false)))
	r.Get("/dl/v/{batch_id}/{filename}", s.handleVirtualTransmission(false))

	r.Method("HEAD", "/inline/v/{batch_id}/{filename}", http.HandlerFunc(s.handleVirtualTransmission(true)))
	r.Get("/inline/v/{batch_id}/{filename}", s.handleVirtualTransmission(true))
	r.Method("HEAD", "/play/v/{batch_id}/{filename}", http.HandlerFunc(s.handleVirtualTransmission(true)))
	r.Get("/play/v/{batch_id}/{filename}", s.handleVirtualTransmission(true))

	r.Get("/watch/v/{batch_id}/{filename}", s.handleVirtualWebPlayer)
	r.Get("/v/{batch_id}/{filename}", s.handleVirtualWebPlayer)
	r.Get("/stream/v/{batch_id}/{filename}", s.handleVirtualWebPlayer)

	// Stremio Add-on Endpoints
	r.Get("/stremio/manifest.json", s.handleStremioManifest)
	r.Get("/stremio/stream/{type}/{id}.json", s.handleStremioStream)

	// Static Assets (Embedded JPlayer dist)
	s.registerStaticRoutes()
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Range, Content-Length, Accept-Ranges")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Telegram.BotUsername != "" {
		http.Redirect(w, r, "https://t.me/"+s.cfg.Telegram.BotUsername, http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"service": "FileStreamBot-Go",
		"status":  "running",
		"version": s.cfg.Server.Version,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "healthy",
		"version": s.cfg.Server.Version,
		"cache":   s.cache.GetStats(),
	})
}

func (s *Server) handleFileInfo(w http.ResponseWriter, r *http.Request) {
	fileIDStr := chi.URLParam(r, "file_id")
	fileID, ok := security.DecodeFileSlug(fileIDStr, s.cfg.Telegram.BotToken)
	if !ok {
		http.Error(w, "Invalid file token", http.StatusBadRequest)
		return
	}

	props, err := s.fastStreamer.GetFileProperties(r.Context(), fileID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	base := s.getBaseURL(r)
	slug := security.EncodeFileSlug(fileID, s.cfg.Telegram.BotToken)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"file_id":        fileID,
		"filename":       props.Name,
		"size_bytes":     props.Size,
		"size_formatted": telegram.FormatBytes(props.Size),
		"mime_type":      props.MimeType,
		"stream_url":     fmt.Sprintf("%s/inline/%s/%s", base, slug, telegram.CleanURLFilename(props.Name)),
		"download_url":   fmt.Sprintf("%s/dl/%s/%s", base, slug, telegram.CleanURLFilename(props.Name)),
		"web_player_url": fmt.Sprintf("%s/watch/%s/%s?v=v2", base, slug, telegram.CleanURLFilename(props.Name)),
	})
}

func (s *Server) handleMediaTransmission(isInline bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fileIDStr := chi.URLParam(r, "file_id")
		fileID, ok := security.DecodeFileSlug(fileIDStr, s.cfg.Telegram.BotToken)
		if !ok {
			http.Error(w, "Invalid file token", http.StatusBadRequest)
			return
		}

		filename := chi.URLParam(r, "filename")

		props, err := s.fastStreamer.GetFileProperties(r.Context(), fileID)
		if err != nil {
			http.Error(w, "File not found: "+err.Error(), http.StatusNotFound)
			return
		}

		if filename == "" {
			filename = props.Name
		}

		disposition := "attachment"
		if isInline {
			disposition = "inline"
		}

		fileSize := props.Size
		w.Header().Set("Content-Type", props.MimeType)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, filename))
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.WriteHeader(http.StatusOK)
			return
		}

		start, end, statusCode, err := parseRange(r.Header.Get("Range"), fileSize)
		if err != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}

		contentLength := end - start + 1
		if statusCode == http.StatusPartialContent {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
			w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.WriteHeader(http.StatusOK)
		}

		err = s.fastStreamer.StreamFileRange(r.Context(), w, fileID, start, end, contentLength, props)
		if err != nil && r.Context().Err() == nil && !strings.Contains(err.Error(), "context canceled") {
			slog.Error("Stream transmission error", "file_id", fileID, "error", err)
		}
	}
}

func (s *Server) handleWebPlayer(w http.ResponseWriter, r *http.Request) {
	fileIDStr := chi.URLParam(r, "file_id")
	fileID, ok := security.DecodeFileSlug(fileIDStr, s.cfg.Telegram.BotToken)
	if !ok {
		http.Error(w, "Invalid file token", http.StatusBadRequest)
		return
	}

	filename := chi.URLParam(r, "filename")
	props, err := s.fastStreamer.GetFileProperties(r.Context(), fileID)
	if err != nil {
		http.Error(w, "File not found: "+err.Error(), http.StatusNotFound)
		return
	}

	if filename == "" {
		filename = props.Name
	}

	base := s.getBaseURL(r)
	mediaLink := fmt.Sprintf("%s/inline/%s/%s", base, fileIDStr, filename)
	downloadLink := fmt.Sprintf("%s/dl/%s/%s", base, fileIDStr, filename)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("CDN-Cache-Control", "no-store")
	w.Header().Set("Cloudflare-CDN-Cache-Control", "no-store")
	_ = RenderPlayer(w, PlayerData{
		Filename:     filename,
		Filesize:     telegram.FormatBytes(props.Size),
		MimeType:     props.MimeType,
		MediaLink:    mediaLink,
		DownloadLink: downloadLink,
		IsAudio:      strings.HasPrefix(props.MimeType, "audio/"),
		IsMkv:        IsMkv(filename, props.MimeType),
		Ext:          filepath.Ext(filename),
		Version:      s.cfg.Server.Version,
	})
}

// Virtual Multi-Part Handlers
func (s *Server) handleVirtualFileInfo(w http.ResponseWriter, r *http.Request) {
	batchIDStr := chi.URLParam(r, "batch_id")
	rawBatchID := security.DecodeVirtualSlug(batchIDStr, s.cfg.Telegram.BotToken)

	meta, err := s.virtualStream.GetVirtualMetadata(r.Context(), rawBatchID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	base := s.getBaseURL(r)
	slug := security.EncodeVirtualSlug(rawBatchID, s.cfg.Telegram.BotToken)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"batch_id":         slug,
		"filename":         meta.Filename,
		"total_size_bytes": meta.TotalSize,
		"size_formatted":   telegram.FormatBytes(meta.TotalSize),
		"mime_type":        meta.MimeType,
		"parts_count":      len(meta.Parts),
		"stream_url":       fmt.Sprintf("%s/inline/v/%s/%s", base, slug, meta.Filename),
		"download_url":     fmt.Sprintf("%s/dl/v/%s/%s", base, slug, meta.Filename),
		"web_player_url":   fmt.Sprintf("%s/v/%s/%s", base, slug, meta.Filename),
	})
}

func (s *Server) handleVirtualTransmission(isInline bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		batchIDStr := chi.URLParam(r, "batch_id")
		rawBatchID := security.DecodeVirtualSlug(batchIDStr, s.cfg.Telegram.BotToken)
		filename := chi.URLParam(r, "filename")

		meta, err := s.virtualStream.GetVirtualMetadata(r.Context(), rawBatchID)
		if err != nil {
			http.Error(w, "Virtual batch not found: "+err.Error(), http.StatusNotFound)
			return
		}

		if filename == "" {
			filename = meta.Filename
		}

		disposition := "attachment"
		if isInline {
			disposition = "inline"
		}

		fileSize := meta.TotalSize
		w.Header().Set("Content-Type", meta.MimeType)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`%s; filename="%s"`, disposition, filename))
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.WriteHeader(http.StatusOK)
			return
		}

		start, end, statusCode, err := parseRange(r.Header.Get("Range"), fileSize)
		if err != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}

		contentLength := end - start + 1
		if statusCode == http.StatusPartialContent {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
			w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.WriteHeader(http.StatusOK)
		}

		err = s.virtualStream.StreamVirtualRange(r.Context(), w, rawBatchID, start, end, contentLength)
		if err != nil {
			if strings.Contains(err.Error(), "FILE_REFERENCE_EXPIRED") {
				s.virtualStream.InvalidateBatch(rawBatchID)
			}
			if r.Context().Err() == nil && !strings.Contains(err.Error(), "context canceled") {
				slog.Error("Virtual stream error", "batch", rawBatchID, "error", err)
			}
		}
	}
}

func (s *Server) handleVirtualWebPlayer(w http.ResponseWriter, r *http.Request) {
	batchIDStr := chi.URLParam(r, "batch_id")
	rawBatchID := security.DecodeVirtualSlug(batchIDStr, s.cfg.Telegram.BotToken)
	filename := chi.URLParam(r, "filename")

	meta, err := s.virtualStream.GetVirtualMetadata(r.Context(), rawBatchID)
	if err != nil {
		http.Error(w, "Virtual batch not found: "+err.Error(), http.StatusNotFound)
		return
	}

	if filename == "" {
		filename = meta.Filename
	}

	base := s.getBaseURL(r)
	mediaLink := fmt.Sprintf("%s/inline/v/%s/%s", base, batchIDStr, filename)
	downloadLink := fmt.Sprintf("%s/dl/v/%s/%s", base, batchIDStr, filename)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("CDN-Cache-Control", "no-store")
	w.Header().Set("Cloudflare-CDN-Cache-Control", "no-store")
	_ = RenderPlayer(w, PlayerData{
		Filename:     filename,
		Filesize:     telegram.FormatBytes(meta.TotalSize),
		MimeType:     meta.MimeType,
		MediaLink:    mediaLink,
		DownloadLink: downloadLink,
		IsAudio:      strings.HasPrefix(meta.MimeType, "audio/"),
		IsMkv:        IsMkv(filename, meta.MimeType),
		Ext:          filepath.Ext(filename),
		Version:      s.cfg.Server.Version,
	})
}

func parseRange(rangeHdr string, fileSize int64) (start, end int64, status int, err error) {
	if rangeHdr == "" {
		return 0, fileSize - 1, http.StatusOK, nil
	}

	m := rangeHeaderRe.FindStringSubmatch(strings.TrimSpace(rangeHdr))
	if len(m) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid range header format")
	}

	startStr, endStr := m[1], m[2]
	if startStr != "" && endStr != "" {
		start, _ = strconv.ParseInt(startStr, 10, 64)
		end, _ = strconv.ParseInt(endStr, 10, 64)
	} else if startStr != "" {
		start, _ = strconv.ParseInt(startStr, 10, 64)
		end = fileSize - 1
	} else if endStr != "" {
		suffixLen, _ := strconv.ParseInt(endStr, 10, 64)
		start = fileSize - suffixLen
		if start < 0 {
			start = 0
		}
		end = fileSize - 1
	} else {
		return 0, 0, 0, fmt.Errorf("empty range bounds")
	}

	if start > end || start >= fileSize {
		return 0, 0, 0, fmt.Errorf("range not satisfiable")
	}

	if end >= fileSize {
		end = fileSize - 1
	}

	return start, end, http.StatusPartialContent, nil
}

func (s *Server) handleStremioManifest(w http.ResponseWriter, r *http.Request) {
	manifest := map[string]any{
		"id":          "org.iseshu.f2l",
		"version":     "1.0.0",
		"name":        "F2L Telegram Streamer",
		"description": "Stream Telegram videos directly in Stremio with zero latency and full 4K HDR & 5.1 Dolby audio support.",
		"resources":   []string{"stream"},
		"types":       []string{"movie", "series", "other"},
		"catalogs":    []any{},
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_ = json.NewEncoder(w).Encode(manifest)
}

func (s *Server) handleStremioStream(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	baseURL := s.getBaseURL(r)

	var streamURL string
	if strings.HasPrefix(id, "v_") {
		batchID := strings.TrimPrefix(id, "v_")
		streamURL = fmt.Sprintf("%s/inline/v/%s/stream.mkv", baseURL, batchID)
	} else {
		streamURL = fmt.Sprintf("%s/inline/%s/stream.mkv", baseURL, id)
	}

	res := map[string]any{
		"streams": []map[string]any{
			{
				"name":  "⚡ F2L Fast Stream",
				"title": "Telegram MTProto Stream\nRFC 7233 Byte-Range",
				"url":   streamURL,
			},
		},
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_ = json.NewEncoder(w).Encode(res)
}
