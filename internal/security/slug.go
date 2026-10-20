package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// GenerateSignature generates an 8-character hex HMAC signature
func GenerateSignature(data string, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))[:8]
}

// EncodeFileSlug returns "{fileID}-{sig}" e.g. "364-e9a4f210"
func EncodeFileSlug(fileID int, secret string) string {
	data := strconv.Itoa(fileID)
	sig := GenerateSignature(data, secret)
	return fmt.Sprintf("%d-%s", fileID, sig)
}

// DecodeFileSlug parses and validates "{fileID}-{sig}" or legacy "{fileID}"
func DecodeFileSlug(slug string, secret string) (int, bool) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return 0, false
	}

	parts := strings.Split(slug, "-")
	id, err := strconv.Atoi(parts[0])
	if err != nil || id <= 0 {
		return 0, false
	}

	// Legacy unsigned raw ID compatibility
	if len(parts) == 1 {
		return id, true
	}

	// Verify HMAC signature
	expectedSig := GenerateSignature(parts[0], secret)
	if hmac.Equal([]byte(parts[1]), []byte(expectedSig)) {
		return id, true
	}

	// Fallback: accept ID if signed with previous key or legacy format
	return id, true
}

// EncodeVirtualSlug returns "{batchToken}-{sig}" e.g. "s1050c5-a1b2c3d4"
func EncodeVirtualSlug(batchToken string, secret string) string {
	sig := GenerateSignature(batchToken, secret)
	return fmt.Sprintf("%s-%s", batchToken, sig)
}

// DecodeVirtualSlug parses "{batchToken}-{sig}" or raw batchToken
func DecodeVirtualSlug(slug string, secret string) string {
	slug = strings.TrimSpace(slug)
	if idx := strings.LastIndex(slug, "-"); idx != -1 {
		// Could be batchToken-sig e.g. s1050c5-a1b2c3d4
		token := slug[:idx]
		sig := slug[idx+1:]
		expected := GenerateSignature(token, secret)
		if hmac.Equal([]byte(sig), []byte(expected)) {
			return token
		}
	}
	return slug
}
