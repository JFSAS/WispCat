// Package cdn implements ordinary C2C image/file download and command30000
// upload from WeChat4.1.13 (269602), with observed plaintext metadata. It has
// no dependency on account storage or the WeChat RPC layer.
package cdn

import (
	"encoding/json"
	"fmt"
)

// Profile is the legacy IP profile derived by the caller from GetCDNDns.
// Addresses and Ports describe candidates; Download uses the first of each and
// never retries or guesses a different transport. Restrict them at the call site
// to choose another endpoint. A TLS connection can be supplied with DialContext.
type Profile struct {
	Version       uint32
	UIN           uint32
	Province      int32
	City          int32
	ISP           int32
	ClientVersion uint32
	OSType        string
	AuthKey       []byte `json:"-"`
	Addresses     []string
	Ports         []uint16
	// NetType is the caller's native network code. Zero is accepted as an
	// explicit unknown network; this package does not inspect the host OS.
	NetType int32
}

func (p Profile) String() string {
	return fmt.Sprintf("CDN profile{addresses:%d ports:%d credentials:redacted}", len(p.Addresses), len(p.Ports))
}
func (p Profile) GoString() string { return p.String() }
func (p Profile) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AddressCount int  `json:"address_count"`
		PortCount    int  `json:"port_count"`
		HasAuthKey   bool `json:"has_auth_key"`
	}{len(p.Addresses), len(p.Ports), len(p.AuthKey) > 0})
}

// CommonOptions require explicit caller choices for fields whose app-side
// defaults were not yet confirmed. A pointer is required even when all values
// are zero; zero here means a caller choice, not a recovered native default.
type CommonOptions struct {
	ChatType int32
	// AppType, CompressionType, EnableHit and ExistenceCheck belong to the
	// native upload branch. Nonzero values are rejected for this download API.
	AppType          int32
	DownloadBehavior int32
	AutoStart        bool
	MessageFlag      string
	CompressionType  int32
	EnableHit        bool
	ExistenceCheck   bool
	// DownPictureFormat defaults to 1, the native path when no format map entry
	// selects format 2. It is only emitted for image file types 1 through 3.
	DownPictureFormat int32
}

func (o CommonOptions) String() string   { return "CDN common options{message_flag:redacted}" }
func (o CommonOptions) GoString() string { return o.String() }
func (o CommonOptions) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ChatType int32 `json:"chat_type"`
		AppType  int32 `json:"app_type"`
	}{o.ChatType, o.AppType})
}

// DownloadRequest selects a single legacy AES image download (FileType1..3).
// Non-image request branches remain unsupported until their headers are traced.
// FileType has no default:
// the caller must map its message/variant to the native type. FileKey may carry
// the caller's opaque local task key but is not emitted by native image download.
// RawSize is the initial size hint. A valid server rawfilesize replaces that
// hint, as it does in the native normal-response handler.
type DownloadRequest struct {
	FileID   string   `json:"-"`
	AESKey   [16]byte `json:"-"`
	FileType int32
	RawSize  uint64
	FileKey  string `json:"-"`
	// ToUser belongs to the upload branch and must be empty for image download.
	ToUser      string `json:"-"`
	ExpectedMD5 string `json:"-"`
	Common      *CommonOptions
	// Zero generates a nonzero sequence / random task ID. Explicit values are
	// useful for deterministic local tests and parent-side transport probes.
	Sequence uint32
	TaskID   uint64
}

func (r DownloadRequest) String() string {
	return fmt.Sprintf("CDN download{type:%d raw_size_hint:%d secrets:redacted}", r.FileType, r.RawSize)
}
func (r DownloadRequest) GoString() string { return r.String() }
func (r DownloadRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		FileType int32  `json:"file_type"`
		RawSize  uint64 `json:"raw_size_hint"`
	}{r.FileType, r.RawSize})
}

// Packet hides credential-bearing bytes from ordinary fmt/JSON diagnostics.
// Bytes returns a copy for the transport; never print or persist that copy.
type Packet struct {
	data     []byte
	sequence uint32
}

func (p Packet) Bytes() []byte { return append([]byte(nil), p.data...) }
func (p Packet) Body() []byte {
	if len(p.data) < HeaderBytes {
		return nil
	}
	return append([]byte(nil), p.data[HeaderBytes:]...)
}
func (p Packet) Sequence() uint32 { return p.sequence }
func (p Packet) String() string   { return fmt.Sprintf("CDN packet{bytes:%d redacted}", len(p.data)) }
func (p Packet) GoString() string { return p.String() }
func (p Packet) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Bytes int `json:"bytes"`
	}{len(p.data)})
}

// Result contains decrypted bytes only after status, sequence, raw size and MD5
// validation. Neither the data nor its digest is included in diagnostics.
type Result struct {
	Data             []byte `json:"-"`
	RawSize          uint64
	CipherBytes      int
	ChecksumVerified bool
	MetadataKeys     []string
}

func (r Result) String() string {
	return fmt.Sprintf("CDN result{bytes:%d checksum_verified:%t data:redacted}", len(r.Data), r.ChecksumVerified)
}
func (r Result) GoString() string { return r.String() }
func (r Result) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Bytes            int  `json:"bytes"`
		ChecksumVerified bool `json:"checksum_verified"`
	}{len(r.Data), r.ChecksumVerified})
}

// Error contains no server text, request values, paths or raw network errors.
// Cause is restricted to context errors so errors.Is remains useful.
type Error struct {
	Category      string
	Code          int64
	Field         string
	ExpectedBytes uint64
	ReceivedBytes uint64
	cause         error
}

func (e *Error) Error() string {
	if e.Field != "" {
		return "cdn " + e.Category + " (field " + e.Field + ")"
	}
	if e.Code != 0 {
		if name := ServerErrorName(e.Code); name != "" {
			return fmt.Sprintf("cdn %s (code %d, %s)", e.Category, e.Code, name)
		}
		return fmt.Sprintf("cdn %s (code %d)", e.Category, e.Code)
	}
	return "cdn " + e.Category
}
func (e *Error) Unwrap() error { return e.cause }

func fail(category string) error { return &Error{Category: category} }

func invalid(category, field string) error { return &Error{Category: category, Field: field} }

// ServerErrorName uses only explicit cases in native577f384. Unknown codes
// remain unnamed; no raw server text is exposed or inferred from nearby codes.
func ServerErrorName(code int64) string {
	switch code {
	case -5103009:
		return "ERR_ParamNotExist"
	case -5103011:
		return "ERR_InvalidAuthKey"
	case -5103015:
		return "ERR_ParamInvalid"
	case -5103017:
		return "ERR_NoData"
	case -5103028:
		return "ERR_RSAVerNotExist"
	case -5103031:
		return "ERR_RSADecrypt"
	case -5103034:
		return "ERR_AESDecrypt"
	case -5103059:
		return "ERR_ServerFileExpired"
	default:
		return ""
	}
}
