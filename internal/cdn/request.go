package cdn

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"sync/atomic"
)

var sequence atomic.Uint32

// EncodeRequest constructs only the observed legacy, non-safeproto stream
// download. It does not manufacture a meta key or use ordinary login session
// keys as CDN authkey. The returned Packet is redacted in fmt/JSON output.
func EncodeRequest(p Profile, r DownloadRequest) (Packet, error) {
	if err := validateProfile(p); err != nil {
		return Packet{}, err
	}
	if err := validateRequest(r, DefaultMaxBytes); err != nil {
		return Packet{}, err
	}
	return encodeRequest(p, r)
}

func validateProfile(p Profile) error {
	if p.Version == 0 {
		return invalid("invalid_profile", "version")
	}
	if p.UIN == 0 {
		return invalid("invalid_profile", "uin")
	}
	if p.ClientVersion == 0 {
		return invalid("invalid_profile", "client_version")
	}
	if len(p.OSType) == 0 || len(p.OSType) > 256 {
		return invalid("invalid_profile", "os_type")
	}
	if len(p.AuthKey) == 0 || len(p.AuthKey) > 4096 {
		return invalid("invalid_profile", "auth_key")
	}
	return nil
}

func validateRequest(r DownloadRequest, maxBytes int64) error {
	if len(r.FileID) == 0 || len(r.FileID) > 8192 {
		return invalid("invalid_request", "file_id")
	}
	if r.FileType <= 0 {
		return invalid("invalid_request", "file_type")
	}
	// Native56e046c sends small filetype5 through584d11c operation2, whose
	// header uses585c59c. This is explicit SDK-level support, not an inferred
	// automatic appmsg selector or support for large/safe-storage files7/9.
	if r.FileType > 3 && r.FileType != 5 {
		return fail("unsupported_file_type")
	}
	if r.RawSize == 0 {
		return invalid("invalid_request", "raw_size_hint")
	}
	if r.RawSize > uint64(maxBytes) {
		return fail("size_limit")
	}
	if len(r.FileKey) > 256 {
		return invalid("invalid_request", "file_key")
	}
	if r.Common == nil {
		return invalid("invalid_request", "common_options")
	}
	if r.ToUser != "" || r.Common.AppType != 0 || r.Common.CompressionType != 0 || r.Common.EnableHit || r.Common.ExistenceCheck {
		return invalid("invalid_request", "upload_only_options")
	}
	if len(r.Common.MessageFlag) > 4096 {
		return invalid("invalid_request", "message_flag")
	}
	if r.Common.DownPictureFormat < 0 || r.Common.DownPictureFormat > 2 {
		return invalid("invalid_request", "down_picture_format")
	}
	if r.ExpectedMD5 != "" {
		if _, err := parseMD5([]byte(r.ExpectedMD5)); err != nil {
			return invalid("invalid_request", "expected_md5")
		}
	}
	return nil
}

func encodeRequest(p Profile, r DownloadRequest) (Packet, error) {
	seq, taskID, err := makeIdentifiers(r.Sequence, r.TaskID)
	if err != nil {
		return Packet{}, err
	}
	wrapped, err := wrapAESKey(r.AESKey)
	if err != nil {
		return Packet{}, err
	}
	u := func(n uint64) []byte { return []byte(strconv.FormatUint(n, 10)) }
	i := func(n int64) []byte { return []byte(strconv.FormatInt(n, 10)) }
	b := func(v bool) []byte {
		if v {
			return []byte("1")
		}
		return []byte("0")
	}
	options := r.Common
	// Preserve native insertion order; no map iteration or unknown overrides.
	fields := profileFields(p, seq)
	fields = append(fields,
		field{"rsaver", []byte(LegacyRSAVersion)},
		field{"rsavalue", wrapped},
		field{"filetype", i(int64(r.FileType))},
		field{"wxchattype", i(int64(options.ChatType))},
		field{"taskid", u(taskID)},
		field{"totalsize", u((r.RawSize &^ 15) + 16)},
		field{"rawtotalsize", u(r.RawSize)},
		field{"fileid", []byte(r.FileID)},
		field{"lastretcode", []byte("0")},
		field{"cli-quic-flag", []byte("0")},
		field{"wxmsgflag", []byte(options.MessageFlag)},
		field{"wxautostart", b(options.AutoStart)},
		field{"downloadbehavor", i(int64(options.DownloadBehavior))},
	)
	if r.FileType >= 1 && r.FileType <= 3 {
		format := options.DownPictureFormat
		if format == 0 {
			format = 1
		}
		fields = append(fields, field{"downpicformat", i(int64(format))})
	}
	// Image operation4 and C2C stream operation2 write initial offset before
	// returning from PackCommonHeader; the later block is upload-only.
	fields = append(fields, field{"offset", []byte("0")})
	fields = append(fields,
		field{"largesvideo", []byte("0")},
		field{"sourceflag", []byte("0")},
	)
	var body []byte
	for _, f := range fields {
		body = appendField(body, f.key, f.value)
	}
	data, err := makeFrame(body, DownloadCommand, p.UIN)
	if err != nil {
		return Packet{}, err
	}
	return Packet{data: data, sequence: seq}, nil
}

func makeIdentifiers(seq uint32, taskID uint64) (uint32, uint64, error) {
	if seq == 0 {
		for seq == 0 {
			seq = sequence.Add(1)
		}
	}
	if taskID == 0 {
		var b [8]byte
		for taskID == 0 {
			if _, err := rand.Read(b[:]); err != nil {
				return 0, 0, fail("random_unavailable")
			}
			taskID = binary.LittleEndian.Uint64(b[:])
		}
	}
	return seq, taskID, nil
}

func profileFields(p Profile, seq uint32) []field {
	u := func(n uint64) []byte { return []byte(strconv.FormatUint(n, 10)) }
	i := func(n int64) []byte { return []byte(strconv.FormatInt(n, 10)) }
	return []field{
		{"ver", u(uint64(p.Version))},
		{"weixinnum", u(uint64(p.UIN))},
		{"province", i(int64(p.Province))},
		{"city", i(int64(p.City))},
		{"isp", i(int64(p.ISP))},
		{"seq", u(uint64(seq))},
		{"clientversion", u(uint64(p.ClientVersion))},
		{"clientostype", []byte(p.OSType)},
		{"authkey", p.AuthKey},
		{"nettype", i(int64(p.NetType))},
		{"acceptdupack", []byte("1")},
	}
}

func parseMD5(value []byte) ([16]byte, error) {
	var digest [16]byte
	if len(value) != 32 {
		return digest, fail("invalid_checksum")
	}
	if _, err := hex.Decode(digest[:], value); err != nil {
		return digest, fail("invalid_checksum")
	}
	return digest, nil
}
