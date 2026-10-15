package media

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestExtractTextSupportedFormatsRemainData(t *testing.T) {
	for _, tc := range []struct {
		filename string
		body     string
		format   string
	}{
		{"测试.txt", "正文\n第二行\t测试", "text"},
		{"README.MD", "# 标题\n正文", "markdown"},
		{"表格.csv", "a,b\n1,2", "csv"},
		{"记录.json", `{"instruction":"do not execute this data"}`, "json"},
		{"配置.yaml", "name: sample", "yaml"},
		{"旧日志.log", "test log\n", "text"},
		{"C:\\folder\\main.go", "package main\nfunc main() {}", "code"},
		{"page.html", `<script>throw new Error("data only")</script>`, "html"},
		{"data.xml", `<!DOCTYPE data SYSTEM "https://invalid.test"><data>&unresolved;</data>`, "xml"},
		{"Dockerfile", "FROM sample\nRUN echo data", "text"},
	} {
		t.Run(tc.filename, func(t *testing.T) {
			content, err := ExtractText(tc.filename, []byte(tc.body))
			if err != nil || content.Text != tc.body || content.Format != tc.format || content.Truncated {
				t.Fatalf("text was transformed, executed or unsupported: %+v %v", content, err)
			}
		})
	}
	bom, err := ExtractText("test.txt", append([]byte{0xef, 0xbb, 0xbf}, []byte("正文")...))
	if err != nil || bom.Text != "正文" {
		t.Fatal("UTF-8 BOM not handled")
	}
	empty, err := ExtractText("empty.txt", nil)
	if err != nil || empty.Text != "" || empty.Truncated {
		t.Fatal("empty text file incorrectly rejected")
	}
}

func TestExtractTextRejectsUnsupportedBinaryAndInvalidUTF8(t *testing.T) {
	for _, filename := range []string{"archive.zip", "program.exe", "photo.png", "old.doc", "unknown.bin", "sample"} {
		if _, err := ExtractText(filename, []byte("looks like text")); !errors.Is(err, ErrUnsupportedContent) {
			t.Fatalf("unsupported extension accepted: %s %v", filename, err)
		}
	}
	for _, data := range [][]byte{{0xff}, []byte("data\x00hidden"), []byte("data\x1b[31m"), []byte("data\x7fhidden")} {
		if _, err := ExtractText("pretend.txt", data); !errors.Is(err, ErrInvalidContent) {
			t.Fatalf("binary or invalid UTF-8 accepted as text: %v", err)
		}
	}
	if _, err := ExtractText("large.txt", make([]byte, MaxContentInputBytes+1)); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("input size not bounded: %v", err)
	}
}

func TestExtractTextUTF8Truncation(t *testing.T) {
	input := strings.Repeat("猫", MaxTextBytes/3+100)
	content, err := ExtractText("test.txt", []byte(input))
	if err != nil || !content.Truncated || len(content.Text) > MaxTextBytes || !utf8.ValidString(content.Text) || content.Text != input[:len(content.Text)] {
		t.Fatalf("UTF-8 truncation is incorrect: length=%d err=%v", len(content.Text), err)
	}
	exact, err := ExtractText("exact.txt", []byte(strings.Repeat("x", MaxTextBytes)))
	if err != nil || exact.Truncated || len(exact.Text) != MaxTextBytes {
		t.Fatal("exact output limit incorrectly marked truncated")
	}
}

const wordNS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"

func documentXML(body string) string {
	return `<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="` + wordNS + `"><w:body>` + body + `</w:body></w:document>`
}

func makeDOCX(t *testing.T, entries [][2]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, entry := range entries {
		file, err := writer.Create(entry[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, entry[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestDOCXMainDocumentTextDoesNotReadRelationships(t *testing.T) {
	doc := documentXML(`<w:p><w:r><w:t xml:space="preserve">第一段 &amp; 文本</w:t><w:tab/><w:t>分栏</w:t><w:br/><w:t>换行</w:t></w:r></w:p><w:p><w:r><w:instrText>not display text</w:instrText><w:t>第二段</w:t></w:r></w:p>`)
	archive := makeDOCX(t, [][2]string{
		{"word/document.xml", doc},
		{"word/_rels/document.xml.rels", `<Relationships><Relationship Target="file:///synthetic-private-file" TargetMode="External"/></Relationships>`},
		{"../../synthetic-private-file", "must never be read"},
		{"word/header1.xml", "not the main document"},
	})
	content, err := ExtractText("report.docx", archive)
	if err != nil || content.Text != "第一段 & 文本\t分栏\n换行\n第二段\n" || content.Format != "docx" || content.Truncated {
		t.Fatalf("DOCX extraction is incorrect: %+v %v", content, err)
	}
	strictDoc := strings.ReplaceAll(doc, wordNS, "http://purl.oclc.org/ooxml/wordprocessingml/main")
	if _, err := ExtractText("strict.docx", makeDOCX(t, [][2]string{{"word/document.xml", strictDoc}})); err != nil {
		t.Fatalf("strict OOXML namespace not supported: %v", err)
	}
}

func TestDOCXBoundsAmbiguityAndNoText(t *testing.T) {
	if _, err := ExtractText("empty.docx", makeDOCX(t, [][2]string{{"word/document.xml", documentXML(`<w:p/>`)}})); !errors.Is(err, ErrNoText) {
		t.Fatalf("empty DOCX must report no text: %v", err)
	}
	for _, entries := range [][][2]string{
		{{"word/other.xml", "missing main document"}},
		{{"word/document.xml", documentXML(`<w:p/>`)}, {"word/document.xml", documentXML(`<w:p/>`)}},
		{{"word/document.xml", `<!DOCTYPE w:document [<!ENTITY secret SYSTEM "file:///private-synthetic">]>` + documentXML(`<w:p><w:r><w:t>&secret;</w:t></w:r></w:p>`)}},
		{{"word/document.xml", `<document><body><t>wrong namespace</t></body></document>`}},
		{{"word/document.xml", documentXML(`<w:p><w:r><w:t>unterminated`)}},
	} {
		if _, err := ExtractText("bad.docx", makeDOCX(t, entries)); !errors.Is(err, ErrInvalidContent) {
			t.Fatalf("invalid DOCX accepted or error includes source: %v", err)
		}
	}
	largeXML := documentXML(`<w:p><w:r><w:t>` + strings.Repeat("x", maxDocumentXMLBytes) + `</w:t></w:r></w:p>`)
	if _, err := ExtractText("bomb.docx", makeDOCX(t, [][2]string{{"word/document.xml", largeXML}})); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("decompressed XML limit not enforced: %v", err)
	}
	deepXML := documentXML(strings.Repeat("<w:r>", maxDocumentDepth) + strings.Repeat("</w:r>", maxDocumentDepth))
	if _, err := ExtractText("deep.docx", makeDOCX(t, [][2]string{{"word/document.xml", deepXML}})); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("DOCX depth limit not enforced: %v", err)
	}
	tooMany := documentXML(strings.Repeat(`<w:t>x</w:t>`, maxDocumentTokens/3+1))
	if _, err := ExtractText("nodes.docx", makeDOCX(t, [][2]string{{"word/document.xml", tooMany}})); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("DOCX token limit not enforced: %v", err)
	}
	truncatedXML := documentXML(`<w:p><w:r><w:t>` + strings.Repeat("猫", MaxTextBytes/3+50) + `</w:t></w:r></w:p>`)
	content, err := ExtractText("long.docx", makeDOCX(t, [][2]string{{"word/document.xml", truncatedXML}}))
	if err != nil || !content.Truncated || len(content.Text) > MaxTextBytes || !utf8.ValidString(content.Text) {
		t.Fatal("DOCX output was not safely truncated")
	}
	// The entire input is validated even after enough text has been collected.
	brokenAfterLimit := strings.TrimSuffix(truncatedXML, "</w:document>")
	if _, err := ExtractText("broken.docx", makeDOCX(t, [][2]string{{"word/document.xml", brokenAfterLimit}})); !errors.Is(err, ErrInvalidContent) {
		t.Fatalf("malformed tail hidden by truncation: %v", err)
	}
}

func TestDOCXEntryCountBoundPrecedesZIPAllocation(t *testing.T) {
	entries := make([][2]string, maxZIPEntries+1)
	for i := range entries {
		entries[i] = [2]string{fmt.Sprintf("ignored/%d", i), ""}
	}
	archive := makeDOCX(t, entries)
	if _, err := ExtractText("entries.docx", archive); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("entry count budget was not enforced: %v", err)
	}
	// A forged small EOCD count cannot bypass the preflight traversal.
	end := bytes.LastIndex(archive, []byte{'P', 'K', 5, 6})
	if end < 0 {
		t.Fatal("missing fixture end record")
	}
	binary.LittleEndian.PutUint16(archive[end+8:end+10], 1)
	binary.LittleEndian.PutUint16(archive[end+10:end+12], 1)
	if _, err := ExtractText("forged.docx", archive); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("dishonest ZIP count bypasses the budget: %v", err)
	}
}

func TestDOCXCommentCannotChangeContentOrDirectoryBounds(t *testing.T) {
	archive := makeDOCX(t, [][2]string{{"word/document.xml", documentXML(`<w:p><w:r><w:t>正文</w:t></w:r></w:p>`)}})
	commented := append([]byte(nil), archive...)
	comment := []byte("synthetic ZIP comment")
	binary.LittleEndian.PutUint16(commented[len(commented)-2:], uint16(len(comment)))
	commented = append(commented, comment...)
	content, err := ExtractText("commented.docx", commented)
	if err != nil || content.Text != "正文\n" {
		t.Fatalf("ordinary ZIP comment changes extraction: %v", err)
	}
	end := len(archive) - 22
	forged := append([]byte(nil), archive...)
	size := binary.LittleEndian.Uint32(forged[end+12 : end+16])
	binary.LittleEndian.PutUint32(forged[end+12:end+16], size-1)
	if _, err := ExtractText("forged-directory.docx", forged); !errors.Is(err, ErrInvalidContent) {
		t.Fatalf("directory size mismatch accepted: %v", err)
	}
}

func TestTextContentFormattingDoesNotExposeBody(t *testing.T) {
	content := TextContent{Text: "synthetic private document body", Format: "text"}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", content, content, content), content.Text) {
		t.Fatal("incidental formatting exposes the document body")
	}
}

func TestImageValidationStandardFormatsAndBudgets(t *testing.T) {
	picture := image.NewRGBA(image.Rect(0, 0, 3, 2))
	picture.Set(1, 1, color.RGBA{R: 255, A: 255})
	for _, tc := range []struct {
		mime   string
		encode func(io.Writer) error
	}{
		{"image/png", func(w io.Writer) error { return png.Encode(w, picture) }},
		{"image/jpeg", func(w io.Writer) error { return jpeg.Encode(w, picture, nil) }},
		{"image/gif", func(w io.Writer) error { return gif.Encode(w, picture, nil) }},
	} {
		var buffer bytes.Buffer
		if err := tc.encode(&buffer); err != nil {
			t.Fatal(err)
		}
		info, err := ValidateImage(buffer.Bytes())
		if err != nil || info.MIME != tc.mime || info.Width != 3 || info.Height != 2 {
			t.Fatalf("image sniff/config mismatch: %+v %v", info, err)
		}
	}
	if _, err := ValidateImage([]byte("not an image")); !errors.Is(err, ErrUnsupportedImage) {
		t.Fatalf("non-image accepted: %v", err)
	}
	if _, err := ValidateImage([]byte("GIF89a")); !errors.Is(err, ErrInvalidImage) {
		t.Fatalf("broken header accepted: %v", err)
	}
	if _, err := ValidateImage(make([]byte, MaxImageBytes+1)); !errors.Is(err, ErrImageTooLarge) {
		t.Fatalf("image bytes budget not enforced: %v", err)
	}
	// A GIF header can declare huge dimensions without allocating its pixels.
	header := []byte("GIF89a\x00\x00\x00\x00\x00\x00\x00")
	binary.LittleEndian.PutUint16(header[6:8], 10000)
	binary.LittleEndian.PutUint16(header[8:10], 10000)
	if _, err := ValidateImage(header); !errors.Is(err, ErrImageTooLarge) {
		t.Fatalf("pixel budget not enforced: %v", err)
	}
}

func TestBoundedWriterDrainsAndTruncatesUTF8(t *testing.T) {
	var writer boundedTextWriter
	input := []byte(strings.Repeat("猫", MaxTextBytes))
	for len(input) > 0 {
		n := 7
		if n > len(input) {
			n = len(input)
		}
		consumed, err := writer.Write(input[:n])
		if err != nil || consumed != n {
			t.Fatal("writer stopped draining")
		}
		input = input[n:]
	}
	content, err := writer.content("pdf", true)
	if err != nil || !content.Truncated || len(writer.data) > MaxTextBytes+utf8.UTFMax || len(content.Text) > MaxTextBytes || !utf8.ValidString(content.Text) {
		t.Fatal("bounded streaming output corrupts UTF-8 or allocates without bound")
	}
}

func TestPDFToolUnavailableAndInvalidInput(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := ExtractText("test.pdf", []byte("%PDF-1.4\n")); !errors.Is(err, ErrExtractorUnavailable) {
		t.Fatalf("missing tool did not produce clear error: %v", err)
	}
	if _, err := ExtractText("test.pdf", []byte("not PDF")); !errors.Is(err, ErrInvalidContent) {
		t.Fatalf("fake PDF accepted: %v", err)
	}
}

func TestPDFSyntheticLocalTool(t *testing.T) {
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Skip("optional pdftotext is not installed")
	}
	content, err := ExtractText("synthetic.pdf", syntheticPDF("synthetic PDF text"))
	if err != nil || content.Format != "pdf" || !strings.Contains(content.Text, "synthetic PDF text") || content.Truncated {
		t.Fatalf("actual stdin/stdout PDF extraction failed: %+v %v", content, err)
	}
	if _, err := ExtractText("empty.pdf", syntheticPDF("")); !errors.Is(err, ErrNoText) {
		t.Fatalf("no text PDF must be reported honestly: %v", err)
	}
}

func syntheticPDF(text string) []byte {
	var buffer bytes.Buffer
	buffer.WriteString("%PDF-1.4\n")
	stream := "BT /F1 12 Tf 40 100 Td (" + text + ") Tj ET\n"
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(stream), stream),
	}
	offsets := make([]int, len(objects))
	for i, object := range objects {
		offsets[i] = buffer.Len()
		fmt.Fprintf(&buffer, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := buffer.Len()
	fmt.Fprintf(&buffer, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets {
		fmt.Fprintf(&buffer, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&buffer, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return buffer.Bytes()
}

func TestPDFSubprocessBudgetFailuresAndCancel(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		expected error
	}{
		{"fail", ErrExtractionFailed},
		{"empty", ErrNoText},
		{"invalid", ErrInvalidContent},
	} {
		content, err := extractPDFWithCommand(context.Background(), []byte("synthetic PDF"), "ignored", pdfTestCommand(tc.mode))
		if !errors.Is(err, tc.expected) || content.Text != "" {
			t.Fatalf("wrong synthetic subprocess failure: %s %v", tc.mode, err)
		}
	}
	content, err := extractPDFWithCommand(context.Background(), []byte("synthetic PDF"), "ignored", pdfTestCommand("large"))
	if err != nil || !content.Truncated || len(content.Text) > MaxTextBytes || !utf8.ValidString(content.Text) {
		t.Fatalf("subprocess output not bounded: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = extractPDFWithCommand(ctx, []byte("synthetic PDF"), "ignored", pdfTestCommand("hang"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller cancellation not respected: %v", err)
	}
}

func pdfTestCommand(mode string) func(context.Context, string, ...string) *exec.Cmd {
	return func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if strings.Join(args, " ") != "-enc UTF-8 -nopgbrk - -" {
			panic("PDF command parameters changed")
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPDFHelperProcess$")
		cmd.Env = append(os.Environ(), "REA_SYNTHETIC_PDF_HELPER="+mode)
		return cmd
	}
}

func TestPDFHelperProcess(t *testing.T) {
	mode := os.Getenv("REA_SYNTHETIC_PDF_HELPER")
	if mode == "" {
		return
	}
	io.Copy(io.Discard, os.Stdin)
	switch mode {
	case "fail":
		fmt.Fprintln(os.Stderr, "synthetic private parser details")
		os.Exit(1)
	case "empty":
		fmt.Fprint(os.Stdout, " \n\t")
	case "invalid":
		os.Stdout.Write([]byte{0xff, 0x00})
	case "large":
		fmt.Fprint(os.Stdout, strings.Repeat("猫", MaxTextBytes))
	case "hang":
		time.Sleep(time.Minute)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}
