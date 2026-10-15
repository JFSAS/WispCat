package store

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// OutgoingMedia lives only in the encrypted checkpoint. Never serialize this
// record as an API response: it contains private upload keys and references.
type OutgoingMedia struct {
	Binding      string `json:"binding"`
	Name         string `json:"name"`
	MIME         string `json:"mime"`
	Size         int64  `json:"size"`
	Digest       string `json:"digest"`
	NativeID     string `json:"native_id"`
	AESKey       []byte `json:"aes_key"`
	ThumbnailKey []byte `json:"thumbnail_key,omitempty"`
	Stage        string `json:"stage"`
	FailureStep  string `json:"failure_step,omitempty"`
	FileID       string `json:"file_id,omitempty"`
	ThumbnailID  string `json:"thumbnail_id,omitempty"`
	MD5          string `json:"md5,omitempty"`
	UploadSize   uint32 `json:"upload_size,omitempty"`
	ThumbSize    uint32 `json:"thumb_size,omitempty"`
	ThumbWidth   uint32 `json:"thumb_width,omitempty"`
	ThumbHeight  uint32 `json:"thumb_height,omitempty"`
}

func (m OutgoingMedia) String() string   { return "store.OutgoingMedia(redacted)" }
func (m OutgoingMedia) GoString() string { return m.String() }

func restoreOutgoingMedia(data *Data) error {
	for id, j := range data.Jobs {
		if j.Media == nil {
			if j.Kind != "" && j.Kind != "text" {
				return errors.New("media send checkpoint is invalid")
			}
			continue
		}
		m := j.Media
		if !mediaHex(id, 32) || (j.Kind != "image" && j.Kind != "file") || m.Binding != data.Binding ||
			m.Size < 1 || m.Size > 16<<20 || !mediaHex(m.Digest, 64) || len(m.AESKey) != 16 ||
			(j.Kind == "image" && len(m.ThumbnailKey) != 16) || !validOutgoingName(m.Name) ||
			len(m.NativeID) > 1024 || m.NativeID == "" || len(m.FileID) > 8192 || len(m.ThumbnailID) > 8192 {
			return errors.New("media send checkpoint is invalid")
		}
		switch m.Stage {
		case "queued", "uploading", "main_uploaded", "uploaded", "submitting", "completed":
		default:
			return errors.New("media send checkpoint is invalid")
		}
		switch j.Status {
		case "queued", "failed", "unknown", "accepted", "echo_observed", "cancelled":
		default:
			return errors.New("media send checkpoint is invalid")
		}
		if j.Status == "queued" && m.Stage != "queued" {
			return errors.New("media send checkpoint is invalid")
		}
	}
	return nil
}

func validOutgoingName(name string) bool {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return name != "." && name != ".."
}
