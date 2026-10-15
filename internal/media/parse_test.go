package media_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/media"
)

const (
	imageKey  = "00112233445566778899aabbccddeeff"
	thumbKey  = "ffeeddccbbaa99887766554433221100"
	imageMD5  = "abcdefabcdefabcdefabcdefabcdefab"
	originMD5 = "12345678123456781234567812345678"
)

func imageXML() string {
	return fmt.Sprintf(`<?xml version="1.0"?><msg><img aeskey="%s" encryver="1" cdnthumbaeskey="%s" cdnthumburl="%s" cdnthumblength="4104" cdnthumbheight="128" cdnthumbwidth="192" cdnmidheight="0" cdnmidwidth="0" cdnhdheight="0" cdnhdwidth="0" cdnmidimgurl="%s" length="46042" md5="%s" originsourcemd5="%s"><secHashInfoBase64>c3ludGhldGlj</secHashInfoBase64><live><duration>0</duration><size>0</size><hdsize>0</hdsize><stillimagetimems>0</stillimagetimems></live></img><ImgSourceInfo><BizType>0</BizType></ImgSourceInfo></msg>`, imageKey, thumbKey, syntheticCDN("thumb"), syntheticCDN("mid"), imageMD5, originMD5)
}

func syntheticCDN(name string) string {
	value := "synthetic-cdn-" + name + "-"
	return value + strings.Repeat("A", 194-len(value))
}

func TestImageCurrentShapeKeepsReferencesPrivate(t *testing.T) {
	descriptor, err := media.Parse(3, []byte(imageXML()))
	if err != nil {
		t.Fatal(err)
	}
	public := descriptor.Public()
	if public.Kind != media.Image || public.DownloadState != media.NotDownloaded || public.SizeBytes == nil || *public.SizeBytes != 46042 || public.ThumbnailSize == nil || *public.ThumbnailSize != 4104 {
		t.Fatalf("incorrect public metadata: %+v", public)
	}
	if public.Width != 0 || public.Height != 0 || public.ThumbnailWidth != 192 || public.ThumbnailHeight != 128 {
		t.Fatalf("unknown image dimensions must not become thumbnail dimensions: %+v", public)
	}
	ref := descriptor.DownloadReference()
	if !ref.Has(media.Regular) || !ref.Has(media.Thumbnail) || ref.Has(media.Original) {
		t.Fatal("reference availability does not match supplied variants")
	}
	if ref.CDNID(media.Regular) != syntheticCDN("mid") || ref.CDNID(media.Thumbnail) != syntheticCDN("thumb") || ref.MD5() != imageMD5 || ref.OriginMD5() != originMD5 || ref.EncryptionVersion() != 1 {
		t.Fatal("internal reference fields did not survive parsing")
	}
	mainKey, _ := hex.DecodeString(imageKey)
	thumbnailKey, _ := hex.DecodeString(thumbKey)
	if !bytes.Equal(ref.AESKey(media.Regular), mainKey) || !bytes.Equal(ref.AESKey(media.Thumbnail), thumbnailKey) {
		t.Fatal("thumbnail and regular encryption keys were confused")
	}
	if n, ok := ref.SizeBytes(media.Original); ok || n != 0 {
		t.Fatal("absent original length must stay unknown")
	}
	assertRedacted(t, descriptor, ref, imageKey, thumbKey, imageMD5, originMD5, syntheticCDN("mid"), syntheticCDN("thumb"), "secHashInfoBase64", "<msg>")

	// Callers cannot mutate a cached descriptor through an accessor result.
	*public.SizeBytes = 1
	mainKey = ref.AESKey(media.Regular)
	mainKey[0] = 99
	if *descriptor.Public().SizeBytes != 46042 || ref.AESKey(media.Regular)[0] != 0 {
		t.Fatal("accessors expose mutable internal values")
	}
}

func TestImagePrefersOnlyCompleteBoundedOriginal(t *testing.T) {
	for _, tc := range []struct {
		name, attrs string
		want        media.Variant
	}{
		{"available", `cdnbigimgurl="original" hdlength="1024"`, media.Original},
		{"at-limit", fmt.Sprintf(`cdnbigimgurl="original" hdlength="%d"`, media.MaxImageBytes), media.Original},
		{"too-large", fmt.Sprintf(`cdnbigimgurl="original" hdlength="%d"`, media.MaxImageBytes+1), media.Regular},
		{"no-reference", `hdlength="1024"`, media.Regular},
		{"unknown-size", `cdnbigimgurl="original"`, media.Regular},
		{"zero-size", `cdnbigimgurl="original" hdlength="0"`, media.Regular},
	} {
		t.Run(tc.name, func(t *testing.T) {
			xml := strings.Replace(imageXML(), "<img ", "<img "+tc.attrs+" ", 1)
			description, err := media.Parse(3, []byte(xml))
			if err != nil {
				t.Fatal(err)
			}
			if got := description.Public().PreferredVariant; got != tc.want {
				t.Fatalf("preferred variant = %q, want %q", got, tc.want)
			}
		})
	}
	withoutKey := strings.Replace(imageXML(), `aeskey="`+imageKey+`"`, "", 1)
	withoutKey = strings.Replace(withoutKey, "<img ", `<img cdnbigimgurl="original" hdlength="1024" `, 1)
	description, err := media.Parse(3, []byte(withoutKey))
	if err != nil || description.Public().PreferredVariant != media.Regular {
		t.Fatal("original without a decryption key was selected", err)
	}
}

func TestFileClassificationNameAndLargeLength(t *testing.T) {
	xml := `<msg><appmsg appid=""><title>fallback.txt</title><type>6</type><appattach><filename><![CDATA[C:\unsafe\..\测试` + "\t" + `文件.txt]]></filename><totallen>4294967296</totallen><fileext>TXT</fileext><attachid>synthetic-private-attach-id</attachid><cdnattachurl>synthetic-private-file-reference</cdnattachurl><aeskey>` + imageKey + `</aeskey><encryver>1</encryver><md5>` + imageMD5 + `</md5></appattach></appmsg><fromusername>synthetic-owner</fromusername></msg>`
	descriptor, err := media.Parse(49, []byte(xml))
	if err != nil {
		t.Fatal(err)
	}
	public := descriptor.Public()
	if public.Kind != media.File || public.DownloadState != media.NotDownloaded || public.Name != "测试文件.txt" || public.Extension != "txt" || public.SizeBytes == nil || *public.SizeBytes != 4294967296 {
		t.Fatalf("file metadata is incorrect: %+v", public)
	}
	ref := descriptor.DownloadReference()
	if !ref.Has(media.Attachment) || ref.Has(media.Regular) || ref.AttachID() != "synthetic-private-attach-id" || ref.CDNID(media.Attachment) != "synthetic-private-file-reference" {
		t.Fatal("file references missing or misclassified")
	}
	assertRedacted(t, descriptor, ref, imageKey, imageMD5, "synthetic-private-attach-id", "synthetic-private-file-reference", "synthetic-owner", "C:\\unsafe", "<appmsg")
}

func TestFileFallbackAndZeroLength(t *testing.T) {
	descriptor, err := media.Parse(49, []byte(`<msg><appmsg><title><![CDATA[../../报告.txt]]></title><type>6</type><appattach><totallen>0</totallen></appattach></appmsg></msg>`))
	if err != nil {
		t.Fatal(err)
	}
	public := descriptor.Public()
	if public.Name != "报告.txt" || public.SizeBytes == nil || *public.SizeBytes != 0 || descriptor.DownloadReference().Has(media.Attachment) {
		t.Fatal("title fallback, zero-size file or incomplete reference handled incorrectly")
	}
	unknown, err := media.Parse(49, []byte(`<msg><appmsg><type>6</type><appattach/></appmsg></msg>`))
	if err != nil || unknown.Public().Name != "attachment" || unknown.Public().SizeBytes != nil {
		t.Fatal("missing display metadata must not be invented")
	}
}

func TestFileCurrentMD5SiblingAndContradictoryDigest(t *testing.T) {
	xml := `<msg><appmsg><title>test.yaml</title><type>6</type><appattach><totallen>209998</totallen><cdnattachurl>synthetic-file-reference</cdnattachurl><aeskey>` + imageKey + `</aeskey><fileext>yaml</fileext></appattach><md5>` + imageMD5 + `</md5></appmsg></msg>`
	d, err := media.Parse(49, []byte(xml))
	if err != nil || d.DownloadReference().MD5() != imageMD5 {
		t.Fatal("current file digest not parsed", err)
	}
	conflict := strings.Replace(xml, "</appattach>", "<md5>"+originMD5+"</md5></appattach>", 1)
	if _, err := media.Parse(49, []byte(conflict)); err == nil {
		t.Fatal("conflicting file checksums accepted")
	}
}

func TestUnsupportedTypesDoNotBecomeAttachments(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind uint32
		xml  string
	}{
		{"text", 1, "plain text"},
		{"link", 49, `<msg><appmsg><title>链接</title><type>5</type><appattach><totallen>123</totallen></appattach></appmsg></msg>`},
		{"app without type", 49, `<msg><appmsg><title>链接</title><appattach/></appmsg></msg>`},
		{"wrong root", 3, `<img aeskey="` + imageKey + `"/>`},
		{"type image but appmsg", 3, `<msg><appmsg><type>6</type><appattach/></appmsg></msg>`},
		{"wrong numeric inner type", 49, `<msg><appmsg><type>06</type><appattach/></appmsg></msg>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := media.Parse(tc.kind, []byte(tc.xml)); !errors.Is(err, media.ErrUnsupported) {
				t.Fatalf("unsupported message classified as media: %v", err)
			}
		})
	}
}

func TestMalformedXMLDoesNotLeakOrResolveEntities(t *testing.T) {
	for _, input := range []string{
		`<!DOCTYPE msg [<!ENTITY secret SYSTEM "file:///synthetic-secret">]><msg><img aeskey="&secret;"/></msg>`,
		`<msg><img aeskey="&undefinedSyntheticSecret;"/></msg>`,
		`<msg><img aeskey="synthetic-secret"`,
		`<msg><img/></msg><msg><img/></msg>`,
		`<msg><img/></msg>synthetic-secret`,
		`<msg><img/><img/></msg>`,
		`<msg><img length="1" length="2"/></msg>`,
		`<msg xmlns="urn:synthetic"><img/></msg>`,
		`<msg><?processing synthetic-secret?><img/></msg>`,
		string([]byte{0xff}),
	} {
		_, err := media.Parse(3, []byte(input))
		if !errors.Is(err, media.ErrMalformed) {
			t.Fatalf("malformed XML accepted or wrong error category: %v", err)
		}
		if strings.Contains(err.Error(), "synthetic") || strings.Contains(err.Error(), "secret") {
			t.Fatal("parse error embeds private XML")
		}
	}
	for _, input := range []string{
		`<msg><appmsg><type>6</type><type>5</type><appattach/></appmsg></msg>`,
		`<msg><appmsg><type><nested>6</nested></type><appattach/></appmsg></msg>`,
		`<msg><appmsg><type>6</type><appattach><totallen>1</totallen><totallen>2</totallen></appattach></appmsg></msg>`,
		`<msg><appmsg><type>6</type></appmsg></msg>`,
	} {
		if _, err := media.Parse(49, []byte(input)); !errors.Is(err, media.ErrMalformed) {
			t.Fatalf("ambiguous file description accepted: %v", err)
		}
	}
}

func TestInvalidImageValues(t *testing.T) {
	for _, attrs := range []string{
		`length="-1"`, `length="+1"`, `length="1.5"`, `length=" 1"`,
		`length="18446744073709551616"`, `length="1099511627777"`,
		`cdnthumbwidth="1048577"`, `cdnhdheight="-1"`,
		`aeskey="not-hex"`, `aeskey="zz112233445566778899aabbccddeeff"`,
		`md5="too-short"`, `originsourcemd5="zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"`,
		`cdnthumburl="has a space"`, `cdnmidimgurl="contains&#10;newline"`,
		`encryver="4294967296"`,
	} {
		t.Run(attrs, func(t *testing.T) {
			if _, err := media.Parse(3, []byte(`<msg><img `+attrs+`/></msg>`)); !errors.Is(err, media.ErrInvalid) {
				t.Fatalf("invalid attribute accepted: %v", err)
			}
		})
	}
}

func TestInvalidFileValues(t *testing.T) {
	for _, fields := range []string{
		`<totallen>-1</totallen>`, `<totallen>18446744073709551616</totallen>`,
		`<aeskey>bad-key</aeskey>`, `<md5>bad-hash</md5>`,
		`<cdnattachurl>not an opaque reference</cdnattachurl>`,
		`<fileext>../exe</fileext>`, `<fileext>txt;exec</fileext>`,
		`<filename>` + strings.Repeat("a", 256) + `</filename>`,
	} {
		if _, err := media.Parse(49, []byte(`<msg><appmsg><type>6</type><appattach>`+fields+`</appattach></appmsg></msg>`)); !errors.Is(err, media.ErrInvalid) {
			t.Fatalf("invalid file metadata accepted: %v", err)
		}
	}
}

func TestBoundedXML(t *testing.T) {
	for _, input := range []string{
		`<msg><img>` + strings.Repeat("x", media.MaxXMLBytes) + `</img></msg>`,
		`<msg><img>` + strings.Repeat("<nested>", 16) + strings.Repeat("</nested>", 16) + `</img></msg>`,
		`<msg><img/>` + strings.Repeat("<unused/>", 2100) + `</msg>`,
	} {
		if _, err := media.Parse(3, []byte(input)); !errors.Is(err, media.ErrTooLarge) {
			t.Fatalf("parser limit not enforced: %v", err)
		}
	}
	var attrs strings.Builder
	for i := 0; i < 513; i++ {
		fmt.Fprintf(&attrs, ` a%d="1"`, i)
	}
	if _, err := media.Parse(3, []byte(`<msg><img`+attrs.String()+`/></msg>`)); !errors.Is(err, media.ErrTooLarge) {
		t.Fatalf("attribute count limit not enforced: %v", err)
	}
}

func assertRedacted(t *testing.T, descriptor media.Descriptor, reference media.Reference, secrets ...string) {
	t.Helper()
	for _, value := range []any{descriptor, &descriptor, reference, &reference} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		formatted := string(encoded) + fmt.Sprintf("%v %+v %#v", value, value, value)
		for _, secret := range secrets {
			if strings.Contains(formatted, secret) {
				t.Fatal("public serialization or log formatting exposes a private reference")
			}
		}
	}
}
