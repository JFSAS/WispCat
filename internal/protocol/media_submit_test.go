package protocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func syntheticSubmissionIdentity() (Bootstrap, Profile) {
	return Bootstrap{Username: []byte("synthetic_owner"), UIN: 123, DeviceID: []byte("0123456789abcdef-extra")},
		Profile{Version: 0x8137ce63, DeviceType: "synthetic-platform"}
}

func syntheticImageSubmission() ImageSubmission {
	return ImageSubmission{Recipient: "filehelper", ClientID: "synthetic-stable-image-id", Metadata: ImageSubmissionMetadata{
		MidReference: "SYNTHETIC_MID_REFERENCE", ThumbnailReference: "SYNTHETIC_THUMB_REFERENCE",
		AESKey: []byte("SYNTHETICKEY1234"), ThumbnailAESKey: []byte("THUMBKEY12345678"),
		MD5: "0123456789abcdef0123456789abcdef", MidLength: 46042, ThumbnailLength: 4104,
		ThumbnailWidth: 120, ThumbnailHeight: 80, UploadInfoValue: 7, UploadInfoByte: 2,
		ImageModelValue: 3,
	}}
}

func TestImageSubmissionMatchesCurrentCDNCommitBuilder(t *testing.T) {
	b, p := syntheticSubmissionIdentity()
	input := syntheticImageSubmission()
	body, err := EncodeImageSubmission(b, p, 19, input)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	base, err := f.Message(1)
	if err != nil || len(base) != 6 {
		t.Fatal("missing injected BaseRequest")
	}
	zero, _ := base.B(1)
	device, _ := base.B(3)
	version, _ := base.N(4)
	scene, _ := base.N(6)
	if !bytes.Equal(zero, []byte{0}) || !bytes.Equal(device, b.DeviceID[:16]) ||
		version != uint64(int64(int32(p.Version))) || scene != 19 {
		t.Fatal("BaseRequest differs from native identity/profile injection")
	}
	for tag, expected := range map[int]string{2: input.ClientID, 3: string(b.Username), 4: input.Recipient} {
		raw, ok := f.B(tag)
		got, err := Unstring(raw)
		if !ok || err != nil || string(got) != expected {
			t.Fatal("client/from/to string wrapper differs from native")
		}
	}
	for tag, expected := range map[int]uint64{5: 4104, 6: 0, 7: 4104, 9: 3, 11: 0, 12: 2, 13: 7, 18: 1, 19: 0,
		20: 46042, 22: 4104, 23: 80, 24: 120, 28: 3, 30: 2, 36: 0, 41: 0} {
		got, ok := f.N(tag)
		if !ok || got != expected {
			t.Fatalf("numeric field %d differs from native CDN submission", tag)
		}
	}
	rawData, _ := f.B(8)
	data, err := Unbuffer(rawData)
	if err != nil || len(data) != 0 {
		t.Fatal("CGI embeds media bytes instead of committing uploaded CDN metadata")
	}
	for tag, expected := range map[int]string{16: input.Metadata.MidReference, 17: hex.EncodeToString(input.Metadata.AESKey),
		21: input.Metadata.ThumbnailReference, 25: hex.EncodeToString(input.Metadata.ThumbnailAESKey), 27: input.Metadata.MD5} {
		got, ok := f.B(tag)
		if !ok || string(got) != expected {
			t.Fatalf("CDN metadata field %d differs from native", tag)
		}
	}
	for _, tag := range []int{10, 14, 26, 29, 31, 40, 44, 45, 48, 56, 57, 58} {
		if len(f[tag]) != 0 {
			t.Fatalf("unassigned native field %d was invented", tag)
		}
	}
	second, err := EncodeImageSubmission(b, p, 19, input)
	if err != nil || !bytes.Equal(body, second) {
		t.Fatal("stable submission ID does not produce a stable request")
	}
}

func TestImageSubmissionRejectsIncompleteOrInconsistentMetadata(t *testing.T) {
	b, p := syntheticSubmissionIdentity()
	for _, mutate := range []func(*ImageSubmission){
		func(s *ImageSubmission) { s.ClientID = "" },
		func(s *ImageSubmission) { s.Metadata.AESKey = []byte("short") },
		func(s *ImageSubmission) { s.Metadata.MidReference = "" },
		func(s *ImageSubmission) { s.Metadata.MD5 = strings.Repeat("z", 32) },
		func(s *ImageSubmission) { s.Metadata.ThumbnailAESKey = nil },
		func(s *ImageSubmission) { s.Metadata.ThumbnailLength = 0 },
		func(s *ImageSubmission) { s.Metadata.ThumbnailHeight = 0 },
		func(s *ImageSubmission) { s.Metadata.FullLength = 1 },
	} {
		input := syntheticImageSubmission()
		mutate(&input)
		if _, err := EncodeImageSubmission(b, p, 19, input); !errors.Is(err, ErrWire) {
			t.Fatal("incomplete media metadata reached the wire encoder")
		}
	}
}

func TestFileSubmissionUsesCommand222Envelope(t *testing.T) {
	b, p := syntheticSubmissionIdentity()
	input := FileSubmission{Recipient: "filehelper", ClientID: "synthetic-file-id", AppMessageXML: "<appmsg><title>test.txt</title><type>6</type></appmsg>",
		CreateTime: 123456, MD5: "0123456789abcdef0123456789abcdef", FileLength: 42, UploadInfoByte: 2, ModelValue: 3}
	body, err := EncodeFileSubmission(b, p, 20, input)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := Parse(body)
	app, err := f.Message(2)
	if err != nil {
		t.Fatal(err)
	}
	for tag, expected := range map[int]string{1: string(b.Username), 2: "", 4: input.Recipient, 6: input.AppMessageXML, 8: input.ClientID, 15: ""} {
		got, ok := app.B(tag)
		if !ok || string(got) != expected {
			t.Fatalf("file AppMsg field %d differs from native", tag)
		}
	}
	for tag, expected := range map[int]uint64{3: 0, 5: 6, 7: 123456, 10: 1} {
		got, ok := app.N(tag)
		if !ok || got != expected {
			t.Fatalf("file AppMsg field %d differs from native", tag)
		}
	}
	if len(f[6]) != 0 {
		t.Fatal("small file receives big-file indicator")
	}
	input.FileLength = 0x1900001
	body, err = EncodeFileSubmission(b, p, 20, input)
	f, _ = Parse(body)
	flag, present := f.N(6)
	if err != nil || !present || flag != 7 {
		t.Fatal("native big-file threshold is not preserved")
	}
	for _, value := range []string{"<appmsg><type>49</type></appmsg>", "<appmsg><type>6</type><type>6</type></appmsg>",
		"<appmsg><type>6</type>", "<appmsg><type>6</type></appmsg><other/>", "<!DOCTYPE appmsg><appmsg><type>6</type></appmsg>"} {
		input.AppMessageXML = value
		if _, err := EncodeFileSubmission(b, p, 20, input); !errors.Is(err, ErrWire) {
			t.Fatal("non-file or ambiguous XML is accepted")
		}
	}
}

func TestMediaSubmissionReceiptsDistinguishCGIAcceptance(t *testing.T) {
	baseOK := Blob(1, Uint(1, 0))
	image := Join(baseOK, Blob(3, String([]byte("synthetic-image-id"))),
		Blob(4, String([]byte("synthetic_owner"))), Blob(5, String([]byte("filehelper"))), Uint(10, 123456789012))
	r, err := DecodeImageSubmissionReceipt(image, "filehelper")
	if err != nil || !r.ServerAccepted || r.ServerID != 123456789012 {
		t.Fatal("native image server identifier was not decoded", err)
	}
	file := Join(baseOK, Uint(9, 987654321012))
	r, err = DecodeFileSubmissionReceipt(file)
	if err != nil || !r.ServerAccepted || r.ServerID != 987654321012 {
		t.Fatal("native file server identifier was not decoded", err)
	}
	code := int64(-31)
	for _, raw := range [][]byte{nil, Join(Blob(1, Uint(1, uint64(code))), Uint(10, 1)),
		Join(baseOK, Uint(10, 0)), Join(baseOK, Uint(10, 1), Uint(10, 2)),
		Join(Blob(1, Uint(1, 1<<32)), Uint(10, 1)), Join(baseOK, Blob(5, String([]byte("other"))), Uint(10, 1))} {
		if result, err := DecodeImageSubmissionReceipt(raw, "filehelper"); err == nil || result.ServerAccepted {
			t.Fatal("invalid, rejected or miscorrelated response is presented as accepted")
		}
	}
	if _, err := DecodeFileSubmissionReceipt(Join(baseOK, Uint(10, 42))); !errors.Is(err, ErrWire) {
		t.Fatal("file CGI mistakenly uses image receipt tag10")
	}
}

func TestImageReceiptResponseSenderAndRecipientGoldenLayout(t *testing.T) {
	// Independent raw-wire fixture for the current response shape: BaseResponse
	// ret0; legacy msgid17; ClientImgID="image-id"; FromUser="owner_example";
	// ToUser="filehelper"; NewMsgID42. No request encoder creates this fixture.
	// The distinct field3/4/5 roles match native response offsets16/24/40 and
	// the anonymized current110 observation, avoiding request-layout symmetry.
	raw, err := hex.DecodeString("0a02080010111a0a0a08696d6167652d6964220f0a0d6f776e65725f6578616d706c652a0c0a0a66696c6568656c706572502a")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := DecodeImageSubmissionReceipt(raw, "filehelper")
	if err != nil || !receipt.ServerAccepted || receipt.ServerID != 42 {
		t.Fatal("response sender is mistaken for the requested recipient", err)
	}
	if receipt, err := DecodeImageSubmissionReceipt(raw, "owner_example"); err == nil || receipt.ServerAccepted {
		t.Fatal("response sender can authorize a different recipient")
	}
	for _, target := range [][]byte{
		Uint(5, 0), Blob(5, []byte{0x80}),
		Join(Blob(5, String([]byte("filehelper"))), Blob(5, String([]byte("filehelper")))),
		Blob(5, String([]byte("different_peer"))),
	} {
		response := Join(Blob(1, Uint(1, 0)), Blob(4, String([]byte("filehelper"))), target, Uint(10, 42))
		if receipt, err := DecodeImageSubmissionReceipt(response, "filehelper"); err == nil || receipt.ServerAccepted {
			t.Fatal("malformed or wrong response5 is hidden by matching response4")
		}
	}
}

func TestMediaSubmissionReceiptsApplyNativeDefaultStatus(t *testing.T) {
	for _, status := range [][]byte{nil, Blob(1, nil), Blob(1, Blob(2, []byte("SYNTHETIC_IGNORED_WORDING")))} {
		image, err := DecodeImageSubmissionReceipt(Join(status, Uint(10, 42)), "filehelper")
		if err != nil || !image.ServerAccepted || image.ServerID != 42 {
			t.Fatal("image receipt requires a status the native consumer defaults", err)
		}
		file, err := DecodeFileSubmissionReceipt(Join(status, Uint(9, 43)))
		if err != nil || !file.ServerAccepted || file.ServerID != 43 {
			t.Fatal("file receipt requires a status the native consumer defaults", err)
		}
		if result, err := DecodeImageSubmissionReceipt(status, "filehelper"); err == nil || result.ServerAccepted {
			t.Fatal("default status is confused with proof of server acceptance")
		}
	}
	for _, status := range [][]byte{Uint(1, 0), Join(Blob(1, nil), Blob(1, nil)),
		Blob(1, Blob(1, nil)), Blob(1, Join(Uint(1, 0), Uint(1, 0))), Blob(1, []byte{0x80})} {
		if result, err := DecodeImageSubmissionReceipt(Join(status, Uint(10, 42)), "filehelper"); err == nil || result.ServerAccepted {
			t.Fatal("wrong wire, duplicate or malformed status bypasses the receipt guard")
		}
	}
}

func TestMediaSubmissionDiagnosticsNeverExposePrivateContent(t *testing.T) {
	image := syntheticImageSubmission()
	file := FileSubmission{Recipient: "private-peer", ClientID: "private-client-id", AppMessageXML: "PRIVATE_FILE_XML", ForwardInfo: "PRIVATE_FORWARD_INFO"}
	for _, value := range []any{image, &image, image.Metadata, &image.Metadata, file, &file} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		outputs := []string{string(raw), fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value), fmt.Sprintf("%x", value)}
		for _, output := range outputs {
			for _, secret := range []string{image.ClientID, image.Metadata.MidReference, image.Metadata.ThumbnailReference,
				string(image.Metadata.AESKey), hex.EncodeToString(image.Metadata.AESKey), image.Metadata.MD5,
				file.Recipient, file.ClientID, file.AppMessageXML, file.ForwardInfo} {
				if strings.Contains(output, secret) {
					t.Fatal("submission diagnostics expose a private field")
				}
			}
		}
	}
}

func TestMediaSubmitWithoutSessionDoesNotAccessNetwork(t *testing.T) {
	c := &Client{}
	if _, err := c.SubmitImage(context.Background(), 1, ImageSubmission{}); err == nil {
		t.Fatal("image submit lacks a session guard")
	}
	if _, err := c.SubmitFile(context.Background(), 1, FileSubmission{}); err == nil {
		t.Fatal("file submit lacks a session guard")
	}
}

func TestMediaSubmitEncodingFailureRetainsErrorCategory(t *testing.T) {
	b, p := syntheticSubmissionIdentity()
	c := &Client{Bootstrap: b, State: &Session{Profile: p}}
	_, imageErr := c.SubmitImage(context.Background(), 1, ImageSubmission{})
	_, fileErr := c.SubmitFile(context.Background(), 1, FileSubmission{})
	for _, test := range []struct {
		err   error
		stage string
	}{{imageErr, "media image encode:"}, {fileErr, "media file encode:"}} {
		if !errors.Is(test.err, ErrWire) || !strings.HasPrefix(test.err.Error(), test.stage) {
			t.Fatal("encoding diagnostics lose the stage or original error")
		}
	}
}

func TestFileAppMessageMatchesNativeXMLLocations(t *testing.T) {
	filename := "测试<&.txt"
	key := []byte("SYNTHETICKEY1234")
	digest := "0123456789abcdef0123456789abcdef"
	ref := "SYNTHETIC_FILE_ID<&"
	body, err := BuildFileAppMessage(filename, 42, digest, ref, key, "txt", "synthetic_owner")
	if err != nil {
		t.Fatal(err)
	}
	var app struct {
		Title  string `xml:"title"`
		Type   int    `xml:"type"`
		MD5    string `xml:"md5"`
		Attach struct {
			Size      uint64 `xml:"totallen"`
			Filename  string `xml:"filename"`
			Reference string `xml:"cdnattachurl"`
			AttachID  string `xml:"attachid"`
			Ext       string `xml:"fileext"`
			AES       string `xml:"aeskey"`
			Version   int    `xml:"encryver"`
		} `xml:"appattach"`
	}
	decoder := xml.NewDecoder(strings.NewReader(body))
	if err := decoder.Decode(&app); err != nil {
		t.Fatal("generated appmsg is not valid XML")
	}
	var from struct {
		Value string `xml:",chardata"`
	}
	if err := decoder.Decode(&from); err != nil || from.Value != "synthetic_owner" {
		t.Fatal("native sibling fromusername is missing")
	}
	if app.Type != 6 || app.Title != filename || app.MD5 != digest || app.Attach.Size != 42 ||
		app.Attach.Filename != filename || app.Attach.Reference != ref || app.Attach.AttachID != "@cdn_"+ref+"_"+hex.EncodeToString(key)+"_1" ||
		app.Attach.Ext != "txt" || app.Attach.AES != hex.EncodeToString(key) || app.Attach.Version != 0 {
		t.Fatal("generated XML moves file fields or invents an attachment identifier")
	}
	if !validFileAppXML(body) || strings.Contains(body, filename) || strings.Contains(body, ref) {
		t.Fatal("native file fragment is rejected or XML input is not escaped")
	}
	for _, mutate := range []func(*FileSubmission){
		func(s *FileSubmission) { s.AppMessageXML += "<other/>" },
		func(s *FileSubmission) { s.AppMessageXML += "<fromusername>duplicate</fromusername>" },
	} {
		input := FileSubmission{AppMessageXML: body}
		mutate(&input)
		if validFileAppXML(input.AppMessageXML) {
			t.Fatal("unexpected sibling XML is accepted")
		}
	}
	for _, size := range []uint64{0, 16<<20 + 1} {
		if _, err := BuildFileAppMessage("test.txt", size, digest, ref, key, "txt", "owner"); !errors.Is(err, ErrWire) {
			t.Fatal("file size outside the supported constructor scope is accepted")
		}
	}
	if _, err := BuildFileAppMessage("../test.txt", 1, digest, ref, key, "txt", "owner"); !errors.Is(err, ErrWire) {
		t.Fatal("local path is used as a filename")
	}
}
