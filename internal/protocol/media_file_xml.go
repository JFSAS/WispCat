package protocol

import (
	"encoding/hex"
	"encoding/xml"
	"strings"
)

// BuildFileAppMessage emits the ordinary type6 subset established by the
// current 48eca94 encoder and 48ef90c wrapper. The latter emits appmsg and
// fromusername as sibling XML nodes. HandleUploadResult31a9990 constructs the
// ordinary attachment identifier from the CDN reference and hexadecimal key.
// MD5 is the original file's checksum; AESKey is the file content key.
// The caller retains this returned private XML outside logs and diagnostics.
func BuildFileAppMessage(filename string, size uint64, md5, fileID string, key []byte, extension, owner string) (string, error) {
	if !validMediaText(filename, 1024, false) || strings.ContainsAny(filename, "/\\") ||
		size == 0 || size > 16<<20 || !validMediaDigest(md5) ||
		!validMediaText(fileID, 4096, false) || len(key) != 16 ||
		!validMediaText(extension, 64, true) || strings.ContainsAny(extension, "/\\") ||
		!validMediaText(owner, 512, false) {
		return "", ErrWire
	}
	app := struct {
		XMLName xml.Name `xml:"appmsg"`
		AppID   string   `xml:"appid,attr"`
		SDKVer  uint32   `xml:"sdkver,attr"`
		Title   string   `xml:"title"`
		Type    uint32   `xml:"type"`
		Attach  struct {
			Length    uint64 `xml:"totallen"`
			Filename  string `xml:"filename"`
			AttachID  string `xml:"attachid"`
			Reference string `xml:"cdnattachurl"`
			Extension string `xml:"fileext"`
			AESKey    string `xml:"aeskey"`
			EncryVer  uint32 `xml:"encryver"`
		} `xml:"appattach"`
		MD5 string `xml:"md5"`
	}{Title: filename, Type: 6, MD5: md5}
	app.Attach.Length, app.Attach.Filename = size, filename
	app.Attach.Reference, app.Attach.Extension = fileID, extension
	app.Attach.AttachID = "@cdn_" + fileID + "_" + hex.EncodeToString(key) + "_1"
	// AppExtInfo constructor a61af4 clears the word containing encryver.
	// Ordinary HandleUploadResult31a9990 updates the AES key and CDN reference
	// without changing that default. Do not borrow image XML's version value.
	app.Attach.AESKey = hex.EncodeToString(key)
	encoded, err := xml.Marshal(app)
	if err != nil {
		return "", ErrWire
	}
	from, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"fromusername"`
		Value   string   `xml:",chardata"`
	}{Value: owner})
	if err != nil {
		return "", ErrWire
	}
	return string(append(encoded, from...)), nil
}
