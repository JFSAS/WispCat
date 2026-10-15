package media

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxDeclaredBytes bounds untrusted metadata, not the WeChat transport's file
// limit. No buffer of this size is allocated by this package.
const MaxDeclaredBytes uint64 = 1 << 40

const maxDimension uint64 = 1 << 20

// Parse accepts complete UTF-8 msg XML for an image (3) or a file appmsg (49,
// inner type 6). Unknown message types and ordinary appmsg links are unsupported.
// A successfully parsed descriptor contains metadata and possibly incomplete
// internal references; it does not prove availability or permit a download.
func Parse(messageType uint32, content []byte) (Descriptor, error) {
	if messageType != 3 && messageType != 49 {
		return Descriptor{}, ErrUnsupported
	}
	root, err := parseXML(content)
	if err != nil {
		return Descriptor{}, err
	}
	if root.name != "msg" {
		return Descriptor{}, ErrUnsupported
	}
	if messageType == 3 {
		return parseImage(root)
	}
	return parseFile(root)
}

func parseImage(root *element) (Descriptor, error) {
	img, err := root.child("img")
	if err != nil {
		return Descriptor{}, err
	}
	if img == nil {
		return Descriptor{}, ErrUnsupported
	}
	meta := Metadata{Kind: Image, DownloadState: NotDownloaded}
	ref := Reference{assets: make(map[Variant]assetReference)}
	key, err := parseKey(img.attrs["aeskey"])
	if err != nil {
		return Descriptor{}, err
	}
	thumbKey, err := parseKey(img.attrs["cdnthumbaeskey"])
	if err != nil {
		return Descriptor{}, err
	}
	for _, field := range []struct {
		variant  Variant
		idAttr   string
		sizeAttr string
		key      []byte
	}{
		{Thumbnail, "cdnthumburl", "cdnthumblength", thumbKey},
		{Regular, "cdnmidimgurl", "length", key},
		{Original, "cdnbigimgurl", "hdlength", key},
	} {
		id, err := parseReference(img.attrs[field.idAttr])
		if err != nil {
			return Descriptor{}, err
		}
		size, err := parseSize(img.attrs[field.sizeAttr])
		if err != nil {
			return Descriptor{}, err
		}
		ref.assets[field.variant] = assetReference{id: id, key: append([]byte(nil), field.key...), size: size}
	}
	meta.SizeBytes = cloneSize(ref.assets[Regular].size)
	meta.ThumbnailSize = cloneSize(ref.assets[Thumbnail].size)
	for _, field := range []struct {
		attr   string
		target *uint32
	}{
		{"cdnthumbwidth", &meta.ThumbnailWidth},
		{"cdnthumbheight", &meta.ThumbnailHeight},
		{"cdnmidwidth", &meta.Width},
		{"cdnmidheight", &meta.Height},
		{"cdnhdwidth", &meta.OriginalWidth},
		{"cdnhdheight", &meta.OriginalHeight},
	} {
		value, err := parseDimension(img.attrs[field.attr])
		if err != nil {
			return Descriptor{}, err
		}
		*field.target = value
	}
	if ref.md5, err = parseMD5(img.attrs["md5"]); err != nil {
		return Descriptor{}, err
	}
	if ref.originMD5, err = parseMD5(img.attrs["originsourcemd5"]); err != nil {
		return Descriptor{}, err
	}
	if ref.encryptionVersion, err = parseVersion(img.attrs["encryver"]); err != nil {
		return Descriptor{}, err
	}
	meta.PreferredVariant = Regular
	if size, known := ref.SizeBytes(Original); ref.Has(Original) && known && size > 0 && size <= MaxImageBytes {
		meta.PreferredVariant = Original
	}
	return Descriptor{metadata: meta, reference: ref}, nil
}

func parseFile(root *element) (Descriptor, error) {
	app, err := root.child("appmsg")
	if err != nil {
		return Descriptor{}, err
	}
	if app == nil {
		return Descriptor{}, ErrUnsupported
	}
	typeText, err := app.value("type")
	if err != nil {
		return Descriptor{}, err
	}
	if typeText != "6" {
		return Descriptor{}, ErrUnsupported
	}
	attach, err := app.child("appattach")
	if err != nil {
		return Descriptor{}, err
	}
	if attach == nil {
		return Descriptor{}, ErrMalformed
	}
	fields := make(map[string]string)
	for _, name := range []string{"filename", "totallen", "fileext", "attachid", "cdnattachurl", "aeskey", "encryver", "md5"} {
		value, err := attach.value(name)
		if err != nil {
			return Descriptor{}, err
		}
		fields[name] = value
	}
	title, err := app.value("title")
	if err != nil {
		return Descriptor{}, err
	}
	filename := fields["filename"]
	if filename == "" {
		filename = title
	}
	name, err := sanitizeFilename(filename)
	if err != nil {
		return Descriptor{}, err
	}
	ext, err := parseExtension(fields["fileext"])
	if err != nil {
		return Descriptor{}, err
	}
	size, err := parseSize(fields["totallen"])
	if err != nil {
		return Descriptor{}, err
	}
	key, err := parseKey(fields["aeskey"])
	if err != nil {
		return Descriptor{}, err
	}
	cdnID, err := parseReference(fields["cdnattachurl"])
	if err != nil {
		return Descriptor{}, err
	}
	attachID, err := parseReference(fields["attachid"])
	if err != nil {
		return Descriptor{}, err
	}
	// Ordinary file messages put MD5 beside appattach in appmsg. Preserve
	// compatibility with the older nested spelling, rejecting contradictory
	// digests rather than silently choosing one.
	appMD5, err := app.value("md5")
	if err != nil {
		return Descriptor{}, err
	}
	if appMD5 != "" && fields["md5"] != "" && !strings.EqualFold(appMD5, fields["md5"]) {
		return Descriptor{}, ErrInvalid
	}
	digest := appMD5
	if digest == "" {
		digest = fields["md5"]
	}
	md5, err := parseMD5(digest)
	if err != nil {
		return Descriptor{}, err
	}
	version, err := parseVersion(fields["encryver"])
	if err != nil {
		return Descriptor{}, err
	}
	ref := Reference{
		assets:   map[Variant]assetReference{Attachment: {id: cdnID, key: key, size: cloneSize(size)}},
		attachID: attachID, md5: md5, encryptionVersion: version,
	}
	return Descriptor{metadata: Metadata{Kind: File, DownloadState: NotDownloaded, Name: name, Extension: ext, SizeBytes: size}, reference: ref}, nil
}

func parseUnsigned(value string, max uint64) (uint64, error) {
	if len(value) > 20 {
		return 0, ErrInvalid
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, ErrInvalid
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > max {
		return 0, ErrInvalid
	}
	return n, nil
}

func parseSize(value string) (*uint64, error) {
	if value == "" {
		return nil, nil
	}
	n, err := parseUnsigned(value, MaxDeclaredBytes)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func parseDimension(value string) (uint32, error) {
	if value == "" {
		return 0, nil
	}
	n, err := parseUnsigned(value, maxDimension)
	return uint32(n), err
}

func parseVersion(value string) (uint32, error) {
	if value == "" {
		return 0, nil
	}
	n, err := parseUnsigned(value, 0xffffffff)
	return uint32(n), err
}

func parseKey(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	if len(value) != 32 && len(value) != 64 {
		return nil, ErrInvalid
	}
	key, err := hex.DecodeString(value)
	if err != nil {
		return nil, ErrInvalid
	}
	return key, nil
}

func parseMD5(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if len(value) != 32 {
		return "", ErrInvalid
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", ErrInvalid
	}
	return strings.ToLower(value), nil
}

func parseReference(value string) (string, error) {
	if len(value) > 8192 {
		return "", ErrInvalid
	}
	for _, c := range value {
		if c < 0x21 || c > 0x7e {
			return "", ErrInvalid
		}
	}
	return value, nil
}

func sanitizeFilename(value string) (string, error) {
	if len(value) > 4096 || !utf8.ValidString(value) {
		return "", ErrInvalid
	}
	// Treat both slash forms as paths, regardless of the host platform. Remove
	// control characters and directional formatting before selecting the basename.
	value = strings.Map(func(c rune) rune {
		if unicode.IsControl(c) || unicode.In(c, unicode.Cf) {
			return -1
		}
		if c == '\\' {
			return '/'
		}
		return c
	}, value)
	if slash := strings.LastIndexByte(value, '/'); slash >= 0 {
		value = value[slash+1:]
	}
	value = strings.TrimSpace(value)
	if value == "." || value == ".." {
		value = ""
	}
	if value == "" {
		value = "attachment"
	}
	if len(value) > 1024 || utf8.RuneCountInString(value) > 255 {
		return "", ErrInvalid
	}
	return value, nil
}

func parseExtension(value string) (string, error) {
	if len(value) > 32 {
		return "", ErrInvalid
	}
	for _, c := range value {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return "", ErrInvalid
		}
	}
	return strings.ToLower(value), nil
}
