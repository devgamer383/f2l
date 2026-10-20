package telegram

import (
	"regexp"
	"strings"

	"github.com/gotd/td/telegram/message/entity"
	"github.com/gotd/td/telegram/message/html"
	"github.com/gotd/td/tg"
)

var (
	mdBoldRe   = regexp.MustCompile(`\*\*(.*?)\*\*`)
	mdItalicRe = regexp.MustCompile(`_([^_]+)_`)
	mdCodeRe   = regexp.MustCompile("`([^`]+)`")
	mdLinkRe   = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
)

func MarkdownToHTML(md string) string {
	res := mdLinkRe.ReplaceAllString(md, `<a href="$2">$1</a>`)
	res = mdBoldRe.ReplaceAllString(res, `<b>$1</b>`)
	res = mdCodeRe.ReplaceAllString(res, `<code>$1</code>`)
	res = mdItalicRe.ReplaceAllString(res, `<i>$1</i>`)
	return res
}

// ParseMarkdown converts markdown text into clean text and Telegram entity structures
func ParseMarkdown(md string) (string, []tg.MessageEntityClass) {
	htmlStr := MarkdownToHTML(md)
	var b entity.Builder
	err := html.HTML(strings.NewReader(htmlStr), &b, html.Options{})
	if err != nil {
		return md, nil
	}
	msg, entities := b.Complete()
	return msg, entities
}

// CleanURLFilename converts a filename to a clean, human-readable URL slug by
// replacing spaces with '-' instead of '%20' and converting brackets to avoid Telegram button errors.
func CleanURLFilename(name string) string {
	if name == "" {
		return "file"
	}
	s := strings.TrimSpace(name)
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "[", "(")
	s = strings.ReplaceAll(s, "]", ")")
	s = strings.ReplaceAll(s, "{", "(")
	s = strings.ReplaceAll(s, "}", ")")
	s = strings.ReplaceAll(s, `"`, "")
	s = strings.ReplaceAll(s, `'`, "")
	s = strings.ReplaceAll(s, `<`, "")
	s = strings.ReplaceAll(s, `>`, "")
	s = strings.ReplaceAll(s, "|", "-")
	s = strings.ReplaceAll(s, "\\", "-")
	s = strings.ReplaceAll(s, "/", "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	return s
}
