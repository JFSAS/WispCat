package cdn

import (
	"context"
	"crypto/aes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Native ordinary MM uploads have SDK byte611=0. The dispatch56df7bc passes
// byte611^1 to5846ba4, which overrides the intermediate10000 with30000. Its
// SafeC2CUploadTask vtable9972578 still uses5856548 PackStreamUpload.
const UploadCommand = 30000

// UploadOptions are explicit caller choices for ordinary operation1 command30000
// with legacy plaintext metadata and legacy content headers. No rule-selected
// safeproto, storage, cache-hit or forward-file path is synthesized.
type UploadOptions struct {
	ChatType        int32
	AppType         int32
	CompressionType int32
	FileFormat      int32
	MidFormat       int32
	ImageWidth      int32
	ImageHeight     int32
}

// UploadRequest uploads one original resource unchanged, with no internally
// generated thumbnail. Upload main(type2) and thumb(type3) separately if the
// parent message codec needs both. The caller selects the native file type and
// app flags; files5/7 use these explicitly legacy content headers, not claimed
// rule-selected safeproto. The command is the observed ordinary MM default30000.
type UploadRequest struct {
	Data     []byte   `json:"-"`
	AESKey   [16]byte `json:"-"`
	FileType int32
	FileKey  string `json:"-"`
	ToUser   string `json:"-"`
	Common   *UploadOptions
	// RawCRC is the caller's plaintext CRC32 value for native run+712. The
	// generating app-side file-info path is not silently inferred here.
	RawCRC   *uint32
	Sequence uint32
	TaskID   uint64
}

func (r UploadRequest) String() string {
	return fmt.Sprintf("CDN upload{type:%d bytes:%d secrets:redacted}", r.FileType, len(r.Data))
}
func (r UploadRequest) GoString() string { return r.String() }
func (r UploadRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		FileType int32 `json:"file_type"`
		Bytes    int   `json:"bytes"`
	}{r.FileType, len(r.Data)})
}

// UploadResult distinguishes server acceptance from content-check verification.
// MD5 is the submitted plaintext digest; ChecksumVerified is only true when a
// returned server digest matches it. No result claims chat delivery or read.
type UploadResult struct {
	FileID           string   `json:"-"`
	AESKey           [16]byte `json:"-"`
	Size             uint64
	MD5              string `json:"-"`
	ServerAccepted   bool
	ChecksumVerified bool
	MetadataKeys     []string
}

func (r UploadResult) String() string {
	return fmt.Sprintf("CDN upload result{bytes:%d accepted:%t checksum_verified:%t secrets:redacted}", r.Size, r.ServerAccepted, r.ChecksumVerified)
}
func (r UploadResult) GoString() string { return r.String() }
func (r UploadResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Size             uint64 `json:"bytes"`
		Accepted         bool   `json:"server_accepted"`
		ChecksumVerified bool   `json:"checksum_verified"`
	}{r.Size, r.ServerAccepted, r.ChecksumVerified})
}

// EncodeUpload follows native5856548 PackStreamUpload. meta_length is computed
// from the FIRST metadata-only buffer, including the25-byte header and filedata
// header; the ciphertext is not included. The second pass adds meta_length.
func EncodeUpload(p Profile, r UploadRequest) (Packet, error) {
	return encodeUpload(p, r, DefaultMaxBytes)
}

func encodeUpload(p Profile, r UploadRequest, maxBytes int64) (Packet, error) {
	if err := validateProfile(p); err != nil {
		return Packet{}, err
	}
	if len(r.Data) == 0 || int64(len(r.Data)) > maxBytes {
		return Packet{}, fail("size_limit")
	}
	if r.FileType < 1 || r.FileType > 7 || r.FileType == 4 || r.FileType == 6 {
		return Packet{}, fail("unsupported_file_type")
	}
	if r.Common == nil || r.RawCRC == nil {
		return Packet{}, invalid("invalid_request", "explicit_upload_options_and_crc")
	}
	if len(r.FileKey) == 0 || len(r.FileKey) > 256 || len(r.ToUser) > 512 {
		return Packet{}, invalid("invalid_request", "local_task_key_or_to_user")
	}
	seq, taskID, err := makeIdentifiers(r.Sequence, r.TaskID)
	if err != nil {
		return Packet{}, err
	}
	wrapped, err := wrapAESKey(r.AESKey)
	if err != nil {
		return Packet{}, err
	}
	ciphertext, err := encryptContent(r.Data, r.AESKey)
	if err != nil {
		return Packet{}, err
	}
	u := func(n uint64) []byte { return []byte(strconv.FormatUint(n, 10)) }
	i := func(n int32) []byte { return []byte(strconv.FormatInt(int64(n), 10)) }
	hash := md5.Sum(r.Data)
	digest := []byte(hex.EncodeToString(hash[:]))
	fields := profileFields(p, seq)
	o := r.Common
	fields = append(fields,
		field{"rsaver", []byte(LegacyRSAVersion)}, field{"rsavalue", wrapped},
		field{"filetype", i(r.FileType)}, field{"wxchattype", i(o.ChatType)},
		field{"taskid", u(taskID)}, field{"totalsize", u(uint64(len(ciphertext)))},
		field{"rawtotalsize", u(uint64(len(r.Data)))},
		field{"lastretcode", []byte("0")}, field{"cli-quic-flag", []byte("0")},
		field{"hasthumb", []byte("0")}, field{"touser", []byte(r.ToUser)},
		field{"compresstype", i(o.CompressionType)}, field{"nocheckaeskey", []byte("1")},
		field{"enablehit", []byte("0")}, field{"existancecheck", []byte("0")},
		field{"apptype", i(o.AppType)}, field{"filekey", []byte(r.FileKey)},
		// CommonHeader's upload tail enters LABEL194, then LABEL249. A
		// supplied raw AES key emits dataid (RunInfo+5088), initially zero;
		// first-block stream uploads also emit aligned offset (RunInfo+736).
		field{"dataid", []byte("0")}, field{"offset", []byte("0")},
		// PackThumbHeader emits these even for hasthumb0, and overwrites the
		// existing hasthumb key rather than producing a duplicate TLV key.
		field{"thumbtotalsize", []byte("0")}, field{"rawthumbsize", []byte("0")},
		field{"rawthumbmd5", nil}, field{"encthumbcrc", []byte("0")},
	)
	if o.ImageWidth > 0 && o.ImageHeight > 0 {
		fields = append(fields, field{"imgheight", i(o.ImageHeight)}, field{"imgwidth", i(o.ImageWidth)})
	}
	fields = append(fields,
		field{"largesvideo", []byte("0")}, field{"sourceflag", []byte("0")},
		field{"advideoflag", []byte("0")},
		field{"filemd5", digest}, field{"rawfilemd5", digest},
		field{"filecrc", u(uint64(*r.RawCRC))},
		field{"file_format", i(o.FileFormat)}, field{"mid_format", i(o.MidFormat)},
		field{"setofpicformat", []byte(imageFormatBits(r.FileType, o.FileFormat, o.MidFormat))},
	)
	var metadata []byte
	for _, f := range fields {
		metadata = appendField(metadata, f.key, f.value)
	}
	// No filedata bytes are in the first buffer: native holds a variant item
	// with its remaining byte count and streams the actual data afterwards.
	firstLength := HeaderBytes + len(metadata) + 4 + len("filedata") + 4
	// Native57792f0 prepends a new meta_length item ahead of the existing
	// vector. This ordering is part of stream-upload framing, not a map.
	metadata = append(appendField(nil, "meta_length", u(uint64(firstLength))), metadata...)
	body := appendField(metadata, "filedata", ciphertext)
	frame, err := makeFrame(body, UploadCommand, p.UIN)
	if err != nil {
		return Packet{}, err
	}
	return Packet{data: frame, sequence: seq}, nil
}

func encryptContent(data []byte, key [16]byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fail("invalid_key")
	}
	pad := aes.BlockSize - len(data)%aes.BlockSize
	ciphertext := make([]byte, len(data)+pad)
	copy(ciphertext, data)
	for offset := len(data); offset < len(ciphertext); offset++ {
		ciphertext[offset] = byte(pad)
	}
	for offset := 0; offset < len(ciphertext); offset += aes.BlockSize {
		block.Encrypt(ciphertext[offset:offset+aes.BlockSize], ciphertext[offset:offset+aes.BlockSize])
	}
	return ciphertext, nil
}

// Values follow585cd5c: ordinary image formats<2 set the JPEG-compatible bits;
// type3 forces the thumb-present bit; non-images have an empty set string.
func imageFormatBits(fileType, format, midFormat int32) string {
	if fileType < 1 || fileType > 3 {
		return ""
	}
	bits := []byte("000000")
	if fileType == 1 {
		if format < 2 {
			bits[0] = '1'
		}
		if midFormat < 2 {
			bits[1] = '1'
		}
		if format == 2 {
			bits[3] = '1'
		}
		if midFormat == 2 {
			bits[4] = '1'
		}
	} else if fileType == 2 {
		if format < 2 {
			bits[1] = '1'
		}
		if format == 2 {
			bits[4] = '1'
		}
	} else {
		bits[2] = '1'
	}
	return string(bits)
}

func (c *Client) Upload(ctx context.Context, p Profile, r UploadRequest) (*UploadResult, error) {
	if ctx == nil {
		return nil, fail("invalid_context")
	}
	timeout, maxBytes := c.Timeout, c.MaxBytes
	if timeout == 0 {
		timeout = 20 * time.Second
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if timeout < 0 || timeout > 5*time.Minute || maxBytes < 1 || maxBytes > 1<<30 {
		return nil, fail("invalid_config")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	packet, err := encodeUpload(p, r, maxBytes)
	if err != nil {
		return nil, err
	}
	var result *UploadResult
	err = c.exchangeFrames(ctx, p, packet, 4096, func(frame []byte) (bool, error) {
		var completed bool
		var decodeErr error
		result, completed, decodeErr = DecodeUploadProgress(frame, r, packet.sequence, maxBytes)
		return completed, decodeErr
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
