package server

import (
	"bytes"
	"strings"
	"testing"
)

func TestRenderPlayer(t *testing.T) {
	data := PlayerData{
		Filename:     "Love.and.Other.Drugs.2010.720p.BluRay.x264.900MB.mkv",
		Filesize:     "900.00 MB",
		MimeType:     "video/x-matroska",
		MediaLink:    "https://file.iseshu.in/inline/364-22080cf5/Love.and.Other.Drugs.2010.720p.BluRay.x264.900MB.mkv",
		DownloadLink: "https://file.iseshu.in/dl/364-22080cf5/Love.and.Other.Drugs.2010.720p.BluRay.x264.900MB.mkv",
		IsAudio:      false,
		IsMkv:        true,
		Ext:          ".mkv",
		Version:      "3.0.0",
	}

	var buf bytes.Buffer
	err := RenderPlayer(&buf, data)
	if err != nil {
		t.Fatalf("RenderPlayer failed: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "JPlayer") {
		t.Errorf("Expected JPlayer in rendered HTML")
	}
	if !strings.Contains(html, "const isMkv = true;") {
		t.Errorf("Expected const isMkv = true; in rendered HTML, got: %s", html[:200])
	}
	if !strings.Contains(html, "IMAX 1.90:1") {
		t.Errorf("Expected IMAX 1.90:1 in rendered HTML")
	}
	if !strings.Contains(html, "Smart SDR Tone Mapping") {
		t.Errorf("Expected Tone Mapping in rendered HTML")
	}
	if strings.Contains(html, "id=\"stream-audio\"") {
		t.Errorf("Expected audio player to NOT be present for video stream")
	}
	if !strings.Contains(html, "id=\"stream-media\"") {
		t.Errorf("Expected video player to be present for video stream")
	}
	if strings.Contains(html, "preload=\"auto\"") {
		t.Errorf("Expected preload=\"metadata\", found preload=\"auto\"")
	}

	// Test audio rendering
	dataAudio := data
	dataAudio.IsAudio = true
	var bufAudio bytes.Buffer
	if err := RenderPlayer(&bufAudio, dataAudio); err != nil {
		t.Fatalf("RenderPlayer audio failed: %v", err)
	}
	htmlAudio := bufAudio.String()
	if !strings.Contains(htmlAudio, "id=\"stream-audio\"") {
		t.Errorf("Expected audio player to be present for audio stream")
	}
	if strings.Contains(htmlAudio, "id=\"stream-media\"") {
		t.Errorf("Expected video player to NOT be present for audio stream")
	}
}
