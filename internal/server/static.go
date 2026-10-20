package server

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticEmbedFS embed.FS

func (s *Server) registerStaticRoutes() {
	sub, err := fs.Sub(staticEmbedFS, "static")
	if err == nil {
		s.router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))
	}
}
