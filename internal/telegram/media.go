package telegram

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gotd/td/tg"
)

type FileProperties struct {
	Name     string
	Size     int64
	MimeType string
	DCID     int
	Location *tg.InputDocumentFileLocation
	Doc      *tg.Document
}

func GetFileProperties(msg *tg.Message) (*FileProperties, error) {
	if msg == nil || msg.Media == nil {
		return nil, fmt.Errorf("message has no media")
	}

	docMedia, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok || docMedia.Document == nil {
		return nil, fmt.Errorf("unsupported media type: expected document/video/audio")
	}

	doc, ok := docMedia.Document.(*tg.Document)
	if !ok {
		return nil, fmt.Errorf("invalid document representation")
	}

	fileName := ""
	for _, attr := range doc.Attributes {
		switch a := attr.(type) {
		case *tg.DocumentAttributeFilename:
			fileName = a.FileName
		case *tg.DocumentAttributeAudio:
			if fileName == "" && a.Title != "" {
				fileName = a.Title + ".mp3"
			}
		}
	}

	mimeType := doc.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	if fileName == "" {
		ext := ".bin"
		if strings.Contains(mimeType, "matroska") {
			ext = ".mkv"
		} else if strings.Contains(mimeType, "mp4") {
			ext = ".mp4"
		} else if strings.Contains(mimeType, "audio") {
			ext = ".mp3"
		}
		fileName = fmt.Sprintf("file_%d%s", doc.ID, ext)
	}

	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}

	return &FileProperties{
		Name:     fileName,
		Size:     doc.Size,
		MimeType: mimeType,
		DCID:     doc.DCID,
		Location: loc,
		Doc:      doc,
	}, nil
}

func FormatBytes(sizeBytes int64) string {
	if sizeBytes <= 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	size := float64(sizeBytes)
	for size >= 1024.0 && i < len(units)-1 {
		size /= 1024.0
		i++
	}
	return fmt.Sprintf("%.2f %s", size, units[i])
}

func MakeProgressBar(current, total int64, width int) (string, float64) {
	if total <= 0 {
		return "[▱▱▱▱▱▱▱▱▱▱]", 0.0
	}
	pct := math.Min(100.0, math.Max(0.0, (float64(current)/float64(total))*100))
	filled := int(math.Round(float64(width) * float64(current) / float64(total)))
	if filled > width {
		filled = width
	}
	if filled < 0 {
		filled = 0
	}
	bar := strings.Repeat("▰", filled) + strings.Repeat("▱", width-filled)
	return fmt.Sprintf("[%s]", bar), pct
}

func FormatETA(seconds float64) string {
	if seconds < 0 || seconds > 86400*30 {
		return "--:--"
	}
	totalSec := int(seconds)
	m := (totalSec % 3600) / 60
	s := totalSec % 60
	h := totalSec / 3600
	if h > 0 {
		return fmt.Sprintf("%02dh %02dm", h, m)
	}
	return fmt.Sprintf("%02dm %02ds", m, s)
}

func RenderProgress(action, filename string, current, total int64, startTime time.Time, extraLine string) string {
	elapsed := math.Max(0.1, time.Since(startTime).Seconds())
	speed := float64(current) / elapsed
	remBytes := math.Max(0, float64(total-current))
	eta := 0.0
	if speed > 0 {
		eta = remBytes / speed
	}
	bar, pct := MakeProgressBar(current, total, 10)

	lines := []string{
		fmt.Sprintf("%s: `%s`", action, filename),
		fmt.Sprintf("%s **%.1f%%**", bar, pct),
		"",
		fmt.Sprintf("⚡ **Speed:** `%s/s` | ⏱️ **ETA:** `%s`", FormatBytes(int64(speed)), FormatETA(eta)),
		fmt.Sprintf("📦 **Processed:** `%s / %s`", FormatBytes(current), FormatBytes(total)),
	}
	if extraLine != "" {
		lines = append(lines, fmt.Sprintf("🧩 **Info:** %s", extraLine))
	}
	return strings.Join(lines, "\n")
}
