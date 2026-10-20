package telegram

import (
	"strings"
	"testing"
)

func TestCleanURLFilename(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "Marty Supreme (2025) BluRay - 1080p - x264 - [Tel _ Tam _ Hin _ Eng] - (DD_5.1 - 192Kbps) - 3.3GB - ESub.mkv",
			expected: "Marty-Supreme-(2025)-BluRay-1080p-x264-(Tel-_-Tam-_-Hin-_-Eng)-(DD_5.1-192Kbps)-3.3GB-ESub.mkv",
		},
		{
			input:    "Love and Other Drugs 2010.mkv",
			expected: "Love-and-Other-Drugs-2010.mkv",
		},
		{
			input:    "Sample [1080p] {Dual Audio}.mp4",
			expected: "Sample-(1080p)-(Dual-Audio).mp4",
		},
		{
			input:    "A   B    C.mkv",
			expected: "A-B-C.mkv",
		},
	}

	for _, tt := range tests {
		got := CleanURLFilename(tt.input)
		if got != tt.expected {
			t.Errorf("CleanURLFilename(%q) = %q; want %q", tt.input, got, tt.expected)
		}
		if strings.Contains(got, "%20") {
			t.Errorf("CleanURLFilename(%q) contains %%20: %q", tt.input, got)
		}
		if strings.Contains(got, " ") {
			t.Errorf("CleanURLFilename(%q) contains spaces: %q", tt.input, got)
		}
		if strings.Contains(got, "[") || strings.Contains(got, "]") {
			t.Errorf("CleanURLFilename(%q) contains brackets: %q", tt.input, got)
		}
	}
}
