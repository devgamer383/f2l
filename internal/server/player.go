package server

import (
	_ "embed"
	"fmt"
	"html/template"
	"io"
	"path/filepath"
	"strings"
)

//go:embed templates/player.html
var playerHTMLTemplate string

type PlayerData struct {
	Filename     string
	Filesize     string
	MimeType     string
	MediaLink    string
	DownloadLink string
	IsAudio      bool
	IsMkv        bool
	Ext          string
	Version      string
}

func RenderPlayer(w io.Writer, data PlayerData) error {
	// Simple token replacement to maintain 100% compatibility with the jinja template
	tmpl := playerHTMLTemplate
	tmpl = strings.ReplaceAll(tmpl, "{{ filename }}", template.HTMLEscapeString(data.Filename))
	tmpl = strings.ReplaceAll(tmpl, "{{ filesize }}", template.HTMLEscapeString(data.Filesize))
	tmpl = strings.ReplaceAll(tmpl, "{{ mimeType }}", template.HTMLEscapeString(data.MimeType))
	tmpl = strings.ReplaceAll(tmpl, "{{ mediaLink }}", template.HTMLEscapeString(data.MediaLink))
	tmpl = strings.ReplaceAll(tmpl, "{{ downloadLink }}", template.HTMLEscapeString(data.DownloadLink))
	tmpl = strings.ReplaceAll(tmpl, "{{ ext }}", template.HTMLEscapeString(strings.ToUpper(strings.TrimPrefix(data.Ext, "."))))
	tmpl = strings.ReplaceAll(tmpl, "{{ version }}", template.HTMLEscapeString(data.Version))

	if data.IsAudio {
		tmpl = strings.ReplaceAll(tmpl, "{% if isAudio %}", "")
		tmpl = strings.ReplaceAll(tmpl, "{% else %}", "<!--")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "-->")
	} else {
		// Video player
		tmpl = strings.ReplaceAll(tmpl, "{% if isAudio %}", "<!--")
		tmpl = strings.ReplaceAll(tmpl, "{% else %}", "-->")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "")
	}

	_, err := fmt.Fprint(w, tmpl)
	return err
}

func IsMkv(filename, mimeType string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".mkv", ".avi", ".flv", ".wmv", ".mov", ".ts":
		return true
	}
	return strings.Contains(mimeType, "matroska")
}
