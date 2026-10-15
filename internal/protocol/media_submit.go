package protocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	ImageSubmitCommand uint16 = 110
	FileSubmitCommand  uint16 = 222
	imageSubmitPath           = "/cgi-bin/micromsg-bin/uploadmsgimg"
	fileSubmitPath            = "/cgi-bin/micromsg-bin/sendappmsg"
)

// ImageSubmission describes media already uploaded to CDN. It is a protocol
// call argument, not a persistence format: JSON and formatting redact content.
// The caller must retain ClientID for the lifetime of a submission attempt.
type ImageSubmission struct {
	Recipient string
	ClientID  string
	Metadata  ImageSubmissionMetadata
}

// ImageSubmissionMetadata follows the ordinary CoSendMessageWithUploadInfo
// branch in 4.1.13. Keys are raw AES bytes; the encoder writes hexadecimal text.
// CDN references are opaque identifiers, not HTTP URLs.
type ImageSubmissionMetadata struct {
	FullReference      string
	MidReference       string
	ThumbnailReference string
	AESKey             []byte
	ThumbnailAESKey    []byte
	MD5                string
	FullLength         uint32
	MidLength          uint32
	ThumbnailLength    uint32
	ThumbnailWidth     uint32
	ThumbnailHeight    uint32
	HEVCMidLength      uint32
	MessageSource      string
	SourceURL          string
	SourceBusinessType uint32
	ExtraText          string
	UploadInfoValue    int32
	UploadInfoByte     uint8
	ImageModelValue    uint32
}

func (s ImageSubmission) String() string             { return "protocol.ImageSubmission(redacted)" }
func (s ImageSubmission) GoString() string           { return s.String() }
func (s ImageSubmission) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }
func (s ImageSubmission) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Kind     string                  `json:"kind"`
		Metadata ImageSubmissionMetadata `json:"metadata"`
	}{"image", s.Metadata})
}
func (m ImageSubmissionMetadata) String() string {
	return "protocol.ImageSubmissionMetadata(redacted)"
}
func (m ImageSubmissionMetadata) GoString() string           { return m.String() }
func (m ImageSubmissionMetadata) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, m.String()) }
func (m ImageSubmissionMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"full_bytes": m.FullLength, "mid_bytes": m.MidLength,
		"thumbnail_bytes": m.ThumbnailLength, "key_bytes": len(m.AESKey),
		"thumbnail_key_bytes": len(m.ThumbnailAESKey), "references_redacted": true,
	})
}

// FileSubmission accepts the appmsg XML produced by the caller's file metadata
// builder. Keeping it explicit avoids inventing XML fields from a protobuf
// type name. Only an appmsg with inner type 6 is allowed by this operation.
type FileSubmission struct {
	Recipient      string
	ClientID       string
	AppID          string
	SDKVersion     uint32
	AppMessageXML  string
	CreateTime     uint32
	MD5            string
	FileLength     uint64
	UploadInfoByte uint8
	ModelValue     uint32
	ForwardInfo    string
}

func (s FileSubmission) String() string             { return "protocol.FileSubmission(redacted)" }
func (s FileSubmission) GoString() string           { return s.String() }
func (s FileSubmission) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }
func (s FileSubmission) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"kind": "file", "file_bytes": s.FileLength, "content_redacted": true})
}

// MediaSubmissionReceipt proves only that the CGI accepted the message and
// returned a server identifier. It does not mean device delivery or read.
type MediaSubmissionReceipt struct {
	ServerID       uint64 `json:"server_id"`
	ServerAccepted bool   `json:"server_accepted"`
}

func (c *Client) SubmitImage(ctx context.Context, taskID uint32, input ImageSubmission) (MediaSubmissionReceipt, error) {
	if c.State == nil {
		return MediaSubmissionReceipt{}, &Error{Category: "session_missing"}
	}
	body, err := EncodeImageSubmission(c.Bootstrap, c.State.Profile, taskID, input)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media image encode: %w", err)
	}
	response, err := c.sessionExchange(ctx, ImageSubmitCommand, imageSubmitPath, body)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media image exchange: %w", err)
	}
	receipt, err := DecodeImageSubmissionReceipt(response, input.Recipient)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media image receipt: %w", err)
	}
	return receipt, nil
}

func (c *Client) SubmitFile(ctx context.Context, taskID uint32, input FileSubmission) (MediaSubmissionReceipt, error) {
	if c.State == nil {
		return MediaSubmissionReceipt{}, &Error{Category: "session_missing"}
	}
	body, err := EncodeFileSubmission(c.Bootstrap, c.State.Profile, taskID, input)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media file encode: %w", err)
	}
	response, err := c.sessionExchange(ctx, FileSubmitCommand, fileSubmitPath, body)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media file exchange: %w", err)
	}
	receipt, err := DecodeFileSubmissionReceipt(response)
	if err != nil {
		return MediaSubmissionReceipt{}, fmt.Errorf("media file receipt: %w", err)
	}
	return receipt, nil
}

// EncodeImageSubmission is bound to the current command110 ordinary CDN
// submission builder (2d4e984, 2d5f128, 4d00e40). Unassigned fields remain unset.
func EncodeImageSubmission(b Bootstrap, p Profile, taskID uint32, input ImageSubmission) ([]byte, error) {
	if !validMediaIdentity(b, p, taskID, input.Recipient, input.ClientID) || !validImageSubmission(input.Metadata) {
		return nil, ErrWire
	}
	m := input.Metadata
	body := Join(
		cdnDiscoveryRequest(b, p, taskID),
		Blob(2, String([]byte(input.ClientID))), Blob(3, String(b.Username)),
		Blob(4, String([]byte(input.Recipient))),
		Uint(5, uint64(m.ThumbnailLength)), Uint(6, 0), Uint(7, uint64(m.ThumbnailLength)),
		Blob(8, Buffer(nil)), Uint(9, 3),
	)
	if m.MessageSource != "" {
		body = append(body, Blob(10, []byte(m.MessageSource))...)
	}
	body = append(body, Join(
		Uint(11, mediaBool(m.FullLength != 0)), Uint(12, 2),
		Uint(13, uint64(int64(m.UploadInfoValue))),
		Blob(15, []byte(m.FullReference)), Blob(16, []byte(m.MidReference)),
		Blob(17, []byte(hex.EncodeToString(m.AESKey))), Uint(18, 1),
		Uint(19, uint64(m.FullLength)), Uint(20, uint64(m.MidLength)),
		Blob(21, []byte(m.ThumbnailReference)), Uint(22, uint64(m.ThumbnailLength)),
		Uint(23, uint64(m.ThumbnailHeight)), Uint(24, uint64(m.ThumbnailWidth)),
		Blob(25, []byte(hex.EncodeToString(m.ThumbnailAESKey))),
		Blob(27, []byte(m.MD5)), Uint(28, uint64(m.ImageModelValue)),
		Uint(30, uint64(m.UploadInfoByte)), Uint(36, mediaBool(m.HEVCMidLength != 0)),
	)...)
	if m.SourceURL != "" {
		body = append(body, Blob(40, []byte(m.SourceURL))...)
	}
	body = append(body, Uint(41, uint64(m.HEVCMidLength))...)
	if m.SourceURL != "" {
		body = append(body, Blob(44, Join(Blob(1, []byte(m.SourceURL)), Uint(2, uint64(m.SourceBusinessType))))...)
	}
	if m.ExtraText != "" {
		body = append(body, Blob(56, []byte(m.ExtraText))...)
	}
	return body, nil
}

// EncodeFileSubmission follows 2d363e0, the current SendAppMsg envelope, and
// the actual command222 descriptor2d00928. It never uses newsendmsg type49.
func EncodeFileSubmission(b Bootstrap, p Profile, taskID uint32, input FileSubmission) ([]byte, error) {
	if !validMediaIdentity(b, p, taskID, input.Recipient, input.ClientID) ||
		input.CreateTime == 0 || input.FileLength == 0 || input.FileLength > math.MaxUint32 ||
		!validMediaDigest(input.MD5) || !validMediaText(input.AppID, 512, true) ||
		!validMediaText(input.ForwardInfo, 64<<10, true) || !validFileAppXML(input.AppMessageXML) {
		return nil, ErrWire
	}
	payload := Join(
		Blob(1, b.Username), Blob(2, []byte(input.AppID)), Uint(3, uint64(input.SDKVersion)),
		Blob(4, []byte(input.Recipient)), Uint(5, 6), Blob(6, []byte(input.AppMessageXML)),
		Uint(7, uint64(input.CreateTime)), Blob(8, []byte(input.ClientID)), Uint(10, 1),
	)
	if input.ForwardInfo != "" {
		payload = append(payload, Blob(13, []byte(input.ForwardInfo))...)
		payload = append(payload, Blob(14, []byte(input.ForwardInfo))...)
	}
	payload = append(payload, Blob(15, nil)...)
	body := Join(cdnDiscoveryRequest(b, p, taskID), Blob(2, payload), Uint(4, 2), Blob(5, []byte(input.MD5)))
	if input.FileLength > 0x1900000 {
		body = append(body, Uint(6, 7)...)
	}
	body = append(body, Join(Uint(9, uint64(input.UploadInfoByte)), Uint(10, uint64(input.ModelValue)))...)
	return body, nil
}

func validMediaIdentity(b Bootstrap, p Profile, taskID uint32, recipient, clientID string) bool {
	return b.UIN != 0 && len(b.DeviceID) >= 16 && taskID != 0 &&
		validMediaText(string(b.Username), 512, false) && validMediaText(p.DeviceType, 512, false) &&
		validMediaText(recipient, 512, false) && validMediaText(clientID, 512, false)
}

func validImageSubmission(m ImageSubmissionMetadata) bool {
	if len(m.AESKey) != 16 || !validMediaDigest(m.MD5) ||
		!validMediaText(m.MidReference, 4096, false) || m.MidLength == 0 ||
		!validMediaText(m.FullReference, 4096, true) ||
		!validMediaText(m.ThumbnailReference, 4096, true) ||
		!validMediaText(m.MessageSource, 64<<10, true) || !validMediaText(m.SourceURL, 8192, true) ||
		!validMediaText(m.ExtraText, 64<<10, true) {
		return false
	}
	for _, value := range []uint32{m.FullLength, m.MidLength, m.ThumbnailLength, m.ThumbnailWidth, m.ThumbnailHeight} {
		if value > math.MaxInt32 {
			return false
		}
	}
	if m.FullLength != 0 && m.FullReference == "" {
		return false
	}
	if m.ThumbnailLength != 0 || m.ThumbnailReference != "" || len(m.ThumbnailAESKey) != 0 {
		return m.ThumbnailLength != 0 && m.ThumbnailReference != "" && len(m.ThumbnailAESKey) == 16 &&
			m.ThumbnailWidth != 0 && m.ThumbnailHeight != 0
	}
	return m.ThumbnailWidth == 0 && m.ThumbnailHeight == 0
}

func validMediaDigest(value string) bool {
	if len(value) != 32 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validMediaText(value string, maxBytes int, allowEmpty bool) bool {
	return (allowEmpty || value != "") && len(value) <= maxBytes && utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\x00\r\n")
}
func mediaBool(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func validFileAppXML(value string) bool {
	if len(value) == 0 || len(value) > 1<<20 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return false
	}
	decoder := xml.NewDecoder(strings.NewReader(value))
	var stack []string
	appCount, typeCount, roots := 0, 0, 0
	fragment := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return len(stack) == 0 && appCount == 1 && typeCount == 1
		}
		if err != nil {
			return false
		}
		switch t := token.(type) {
		case xml.StartElement:
			if len(stack) == 0 {
				roots++
				if roots == 1 {
					if t.Name.Local != "msg" && t.Name.Local != "appmsg" {
						return false
					}
					fragment = t.Name.Local == "appmsg"
				} else if roots != 2 || !fragment || t.Name.Local != "fromusername" {
					return false
				}
				if roots == 2 {
					var username string
					if err := decoder.DecodeElement(&username, &t); err != nil || !validMediaText(username, 512, true) {
						return false
					}
					continue
				}
			}
			if len(stack) >= 64 {
				return false
			}
			if t.Name.Local == "appmsg" {
				if len(stack) > 0 && (len(stack) != 1 || stack[0] != "msg") {
					return false
				}
				appCount++
			}
			if t.Name.Local == "type" && len(stack) > 0 && stack[len(stack)-1] == "appmsg" {
				var kind string
				if err := decoder.DecodeElement(&kind, &t); err != nil || strings.TrimSpace(kind) != "6" {
					return false
				}
				typeCount++
				continue
			}
			stack = append(stack, t.Name.Local)
		case xml.EndElement:
			if len(stack) == 0 {
				return false
			}
			stack = stack[:len(stack)-1]
		case xml.Directive:
			return false
		}
	}
}

// mediaCGIFields reads only this CGI's root1.BaseResponse tag1 status. Native
// response constructors default ret to zero when Root1/ret is absent. A
// nonzero server identifier is still required by mediaServerReceipt; missing
// status alone never proves acceptance. No error wording is exposed.
func mediaCGIFields(data []byte) (Fields, error) {
	f, err := Parse(data)
	if err != nil {
		return nil, ErrWire
	}
	if len(f[1]) == 0 {
		return f, nil
	}
	if len(f[1]) != 1 || f[1][0].Wire != 2 {
		return nil, ErrWire
	}
	base, err := Parse(f[1][0].Bytes)
	if err != nil {
		return nil, ErrWire
	}
	if len(base[1]) == 0 {
		return f, nil
	}
	if len(base[1]) != 1 || base[1][0].Wire != 0 {
		return nil, ErrWire
	}
	value := base[1][0].Number
	ret := int32(value)
	if value != uint64(uint32(ret)) && value != uint64(int64(ret)) {
		return nil, ErrWire
	}
	if ret != 0 {
		return nil, &Error{Category: "media_submit_rejected", Code: ret}
	}
	return f, nil
}

// UploadMsgImgResponse is not laid out like UploadMsgImgRequest: response3
// contains ClientImgID, response4 contains FromUser, and response5 contains
// ToUser (nested SKString). The native writer4d0392c/parser4d030ac confirm
// fields4/5 at native24/40; the current110 response confirms their roles.
func DecodeImageSubmissionReceipt(data []byte, recipient string) (MediaSubmissionReceipt, error) {
	f, err := mediaCGIFields(data)
	if err != nil {
		return MediaSubmissionReceipt{}, err
	}
	if values := f[5]; len(values) != 0 {
		if len(values) != 1 || values[0].Wire != 2 {
			return MediaSubmissionReceipt{}, ErrWire
		}
		peer, err := Unstring(values[0].Bytes)
		if err != nil || !bytes.Equal(peer, []byte(recipient)) {
			return MediaSubmissionReceipt{}, ErrWire
		}
	}
	return mediaServerReceipt(f, 10)
}

// DecodeFileSubmissionReceipt uses SendAppMsgResponse field9. Its writer
// 4d1596c stores a varint64 from native+56, which 2d363e0 copies to Message+248.
func DecodeFileSubmissionReceipt(data []byte) (MediaSubmissionReceipt, error) {
	f, err := mediaCGIFields(data)
	if err != nil {
		return MediaSubmissionReceipt{}, err
	}
	return mediaServerReceipt(f, 9)
}

func mediaServerReceipt(f Fields, tag int) (MediaSubmissionReceipt, error) {
	values := f[tag]
	if len(values) != 1 || values[0].Wire != 0 || values[0].Number == 0 {
		return MediaSubmissionReceipt{}, ErrWire
	}
	return MediaSubmissionReceipt{ServerID: values[0].Number, ServerAccepted: true}, nil
}
