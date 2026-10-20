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
	tmpl := playerHTMLTemplate
	tmpl = strings.ReplaceAll(tmpl, "{{ filename }}", template.HTMLEscapeString(data.Filename))
	tmpl = strings.ReplaceAll(tmpl, "{{ filesize }}", template.HTMLEscapeString(data.Filesize))
	tmpl = strings.ReplaceAll(tmpl, "{{ mimeType }}", template.HTMLEscapeString(data.MimeType))
	tmpl = strings.ReplaceAll(tmpl, "{{ mediaLink }}", template.HTMLEscapeString(data.MediaLink))
	tmpl = strings.ReplaceAll(tmpl, "{{ downloadLink }}", template.HTMLEscapeString(data.DownloadLink))
	tmpl = strings.ReplaceAll(tmpl, "{{ ext }}", template.HTMLEscapeString(strings.ToUpper(strings.TrimPrefix(data.Ext, "."))))
	tmpl = strings.ReplaceAll(tmpl, "{{ version }}", template.HTMLEscapeString(data.Version))
	tmpl = strings.ReplaceAll(tmpl, "{{ isMkv }}", fmt.Sprintf("%t", data.IsMkv))
	tmpl = strings.ReplaceAll(tmpl, "{{ isAudio }}", fmt.Sprintf("%t", data.IsAudio))

	var mediaContainer string
	if data.IsAudio {
		mediaContainer = fmt.Sprintf(`<div id="audio-container">
        <div class="audio-player-card">
          <div class="audio-icon-box">
            <i class="fa-solid fa-music"></i>
          </div>
          <audio id="stream-audio" controls preload="metadata" style="width: 100%%; max-width: 600px;">
            <source src="%s" type="%s">
            Your browser does not support audio streaming.
          </audio>
        </div>
      </div>`,
			template.HTMLEscapeString(data.MediaLink),
			template.HTMLEscapeString(data.MimeType),
		)
		tmpl = strings.ReplaceAll(tmpl, "{{ audioDisplay }}", "block")
		tmpl = strings.ReplaceAll(tmpl, "{{ videoDisplay }}", "none")
		tmpl = strings.ReplaceAll(tmpl, "{% if isAudio %}", "")
		tmpl = strings.ReplaceAll(tmpl, "{% else %}", "<!--")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "-->")
	} else {
		mediaContainer = fmt.Sprintf(`<div id="video-container">
        <video id="stream-media" controls playsinline preload="metadata">
          <source src="%s" type="%s">
          <p>Your browser does not support HTML5 video streaming. Use VLC or MX Player.</p>
        </video>
      </div>`,
			template.HTMLEscapeString(data.MediaLink),
			template.HTMLEscapeString(data.MimeType),
		)
		tmpl = strings.ReplaceAll(tmpl, "{{ audioDisplay }}", "none")
		tmpl = strings.ReplaceAll(tmpl, "{{ videoDisplay }}", "block")
		tmpl = strings.ReplaceAll(tmpl, "{% if isAudio %}", "<!--")
		tmpl = strings.ReplaceAll(tmpl, "{% else %}", "-->")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "")
	}
	tmpl = strings.ReplaceAll(tmpl, "{{ mediaContainer }}", mediaContainer)

	if data.IsMkv {
		tmpl = strings.ReplaceAll(tmpl, "{{ mkvNoticeDisplay }}", "flex")
		tmpl = strings.ReplaceAll(tmpl, "{% if isMkv %}", "")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "")
	} else {
		tmpl = strings.ReplaceAll(tmpl, "{{ mkvNoticeDisplay }}", "none")
		tmpl = strings.ReplaceAll(tmpl, "{% if isMkv %}", "<!--")
		tmpl = strings.ReplaceAll(tmpl, "{% endif %}", "-->")
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
