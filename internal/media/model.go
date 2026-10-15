// Package media parses bounded WeChat media descriptions. It does not download
// content or assert that a CDN reference is still valid.
package media

import (
	"encoding/json"
	"errors"
	"fmt"
)

const MaxXMLBytes = 128 << 10

var (
	ErrUnsupported = errors.New("unsupported media message")
	ErrMalformed   = errors.New("malformed media description")
	ErrTooLarge    = errors.New("media description exceeds parsing limits")
	ErrInvalid     = errors.New("invalid media metadata")
)

type Kind string

const (
	Image Kind = "image"
	File  Kind = "file"
)

const NotDownloaded = "not_downloaded"

type Variant string

const (
	Thumbnail  Variant = "thumbnail"
	Regular    Variant = "regular"
	Original   Variant = "original"
	Attachment Variant = "attachment"
)

// Metadata contains only display information. Download credentials, source XML,
// sender identifiers and CDN references are deliberately absent.
type Metadata struct {
	Kind             Kind    `json:"kind"`
	DownloadState    string  `json:"download_state"`
	PreferredVariant Variant `json:"preferred_variant,omitempty"`
	Name             string  `json:"name,omitempty"`
	Extension        string  `json:"extension,omitempty"`
	SizeBytes        *uint64 `json:"size_bytes,omitempty"`
	Width            uint32  `json:"width,omitempty"`
	Height           uint32  `json:"height,omitempty"`
	OriginalWidth    uint32  `json:"original_width,omitempty"`
	OriginalHeight   uint32  `json:"original_height,omitempty"`
	ThumbnailSize    *uint64 `json:"thumbnail_size_bytes,omitempty"`
	ThumbnailWidth   uint32  `json:"thumbnail_width,omitempty"`
	ThumbnailHeight  uint32  `json:"thumbnail_height,omitempty"`
}

type assetReference struct {
	id   string
	key  []byte
	size *uint64
}

// Reference is for internal download code. Its accessors explicitly opt into
// reading credentials; JSON and log formatting never expose them.
type Reference struct {
	assets            map[Variant]assetReference
	attachID          string
	md5               string
	originMD5         string
	encryptionVersion uint32
}

func (r Reference) CDNID(variant Variant) string { return r.assets[variant].id }

func (r Reference) AESKey(variant Variant) []byte {
	return append([]byte(nil), r.assets[variant].key...)
}

func (r Reference) SizeBytes(variant Variant) (uint64, bool) {
	v := r.assets[variant].size
	if v == nil {
		return 0, false
	}
	return *v, true
}

func (r Reference) Has(variant Variant) bool {
	v := r.assets[variant]
	return v.id != "" && len(v.key) != 0
}

func (r Reference) AttachID() string             { return r.attachID }
func (r Reference) MD5() string                  { return r.md5 }
func (r Reference) OriginMD5() string            { return r.originMD5 }
func (r Reference) EncryptionVersion() uint32    { return r.encryptionVersion }
func (r Reference) MarshalJSON() ([]byte, error) { return []byte("{}"), nil }
func (r Reference) String() string               { return "media.Reference(redacted)" }
func (r Reference) GoString() string             { return r.String() }

type Descriptor struct {
	metadata  Metadata
	reference Reference
}

func (d Descriptor) Public() Metadata {
	m := d.metadata
	m.SizeBytes = cloneSize(m.SizeBytes)
	m.ThumbnailSize = cloneSize(m.ThumbnailSize)
	return m
}

func (d Descriptor) DownloadReference() Reference { return d.reference }
func (d Descriptor) MarshalJSON() ([]byte, error) { return json.Marshal(d.Public()) }
func (d Descriptor) String() string               { return fmt.Sprintf("media.Descriptor(%s)", d.metadata.Kind) }
func (d Descriptor) GoString() string             { return d.String() }

func cloneSize(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
