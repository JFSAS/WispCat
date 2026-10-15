package media

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os/exec"
	"path"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxTextBytes         = 64 << 10
	MaxContentInputBytes = 32 << 20
	MaxImageBytes        = 20 << 20
	MaxImagePixels       = 50_000_000
	maxDocumentXMLBytes  = 8 << 20
	maxZIPDeclaredBytes  = 64 << 20
	maxZIPEntries        = 4096
	maxDocumentTokens    = 200_000
	maxDocumentDepth     = 128
	pdfTimeout           = 15 * time.Second
)

var (
	ErrUnsupportedContent   = errors.New("unsupported file content format")
	ErrInvalidContent       = errors.New("invalid file content")
	ErrContentTooLarge      = errors.New("file content exceeds extraction budget")
	ErrNoText               = errors.New("file has no extractable text")
	ErrExtractorUnavailable = errors.New("PDF text extractor is unavailable")
	ErrExtractionFailed     = errors.New("PDF text extraction failed")
	ErrExtractionTimeout    = errors.New("PDF text extraction timed out")
	ErrUnsupportedImage     = errors.New("unsupported image format")
	ErrInvalidImage         = errors.New("invalid image header")
	ErrImageTooLarge        = errors.New("image exceeds processing budget")
)

type TextContent struct {
	Text      string `json:"text"`
	Format    string `json:"format"`
	Truncated bool   `json:"truncated"`
}

// Plaintext is deliberately available to a caller constructing model input,
// while incidental formatting must not put document contents into a log.
func (c TextContent) String() string   { return "media.TextContent(redacted)" }
func (c TextContent) GoString() string { return c.String() }

type ImageInfo struct {
	MIME   string `json:"mime"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

// ValidateImage uses the standard library's header decoder. It validates a
// supported encoding and its dimensions, not every pixel or animated frame.
// These limits are local processing budgets, not WeChat transport limits.
func ValidateImage(data []byte) (ImageInfo, error) {
	if len(data) > MaxImageBytes {
		return ImageInfo{}, ErrImageTooLarge
	}
	if len(data) == 0 {
		return ImageInfo{}, ErrInvalidImage
	}
	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		if errors.Is(err, image.ErrFormat) {
			return ImageInfo{}, ErrUnsupportedImage
		}
		return ImageInfo{}, ErrInvalidImage
	}
	mime, ok := map[string]string{"png": "image/png", "jpeg": "image/jpeg", "gif": "image/gif"}[format]
	if !ok {
		return ImageInfo{}, ErrUnsupportedImage
	}
	if cfg.Width <= 0 || cfg.Height <= 0 {
		return ImageInfo{}, ErrInvalidImage
	}
	if uint64(cfg.Width)*uint64(cfg.Height) > MaxImagePixels {
		return ImageInfo{}, ErrImageTooLarge
	}
	return ImageInfo{MIME: mime, Width: cfg.Width, Height: cfg.Height}, nil
}

// ExtractText reads only supplied bytes. It never treats filename as a local
// path, executes file contents, or extracts archives to disk. Text, HTML and code
// are returned as data. PDF support is optional and uses installed pdftotext.
func ExtractText(filename string, data []byte) (TextContent, error) {
	return ExtractTextContext(context.Background(), filename, data)
}

func ExtractTextContext(ctx context.Context, filename string, data []byte) (TextContent, error) {
	if err := ctx.Err(); err != nil {
		return TextContent{}, err
	}
	if len(data) > MaxContentInputBytes {
		return TextContent{}, ErrContentTooLarge
	}
	if len(filename) > 4096 || !utf8.ValidString(filename) {
		return TextContent{}, ErrInvalidContent
	}
	base := strings.ToLower(path.Base(strings.ReplaceAll(filename, "\\", "/")))
	extension := strings.TrimPrefix(path.Ext(base), ".")
	if extension == "docx" {
		return extractDOCX(ctx, data)
	}
	if extension == "pdf" {
		return extractPDF(ctx, data)
	}
	format, ok := textFormat(extension, base)
	if !ok {
		return TextContent{}, ErrUnsupportedContent
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	if !validText(data) {
		return TextContent{}, ErrInvalidContent
	}
	text, truncated := truncateUTF8(data, MaxTextBytes)
	return TextContent{Text: string(text), Format: format, Truncated: truncated}, nil
}

func textFormat(extension, base string) (string, bool) {
	switch extension {
	case "txt", "text", "log", "ini", "conf", "cfg", "toml":
		return "text", true
	case "md", "markdown":
		return "markdown", true
	case "csv", "tsv":
		return extension, true
	case "json", "jsonl", "ndjson":
		return extension, true
	case "yaml", "yml":
		return "yaml", true
	case "xml":
		return "xml", true
	case "html", "htm":
		return "html", true
	case "css", "scss", "sass", "js", "jsx", "mjs", "cjs", "ts", "tsx", "go", "py", "pyi", "rs", "c", "h", "cc", "cpp", "cxx", "hpp", "java", "kt", "kts", "swift", "m", "mm", "rb", "php", "sh", "bash", "zsh", "fish", "ps1", "bat", "cmd", "sql", "r", "lua", "pl", "perl", "tex", "bib", "proto", "graphql", "gql":
		return "code", true
	}
	switch base {
	case "readme", "license", "copying", "dockerfile", "makefile", ".gitignore", ".editorconfig":
		return "text", true
	}
	return "", false
}

func validText(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for _, b := range data {
		if b == 0x7f || (b < 0x20 && b != '\t' && b != '\n' && b != '\r') {
			return false
		}
	}
	return true
}

func truncateUTF8(data []byte, limit int) ([]byte, bool) {
	if len(data) <= limit {
		return data, false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	return data[:end], true
}

func extractDOCX(ctx context.Context, data []byte) (TextContent, error) {
	// NewReader allocates a File for every central-directory entry. Enforce the
	// count before calling it, including when the EOCD count is dishonest.
	data, err := preflightZIP(data)
	if err != nil {
		return TextContent{}, err
	}
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return TextContent{}, ErrInvalidContent
	}
	if len(archive.File) > maxZIPEntries {
		return TextContent{}, ErrContentTooLarge
	}
	var document *zip.File
	var declared uint64
	for _, entry := range archive.File {
		if entry.UncompressedSize64 > maxZIPDeclaredBytes || declared > maxZIPDeclaredBytes-entry.UncompressedSize64 {
			return TextContent{}, ErrContentTooLarge
		}
		declared += entry.UncompressedSize64
		if entry.Name != "word/document.xml" {
			continue
		}
		if document != nil || entry.Flags&1 != 0 || (entry.Method != zip.Store && entry.Method != zip.Deflate) {
			return TextContent{}, ErrInvalidContent
		}
		document = entry
	}
	if document == nil {
		return TextContent{}, ErrInvalidContent
	}
	if document.UncompressedSize64 > maxDocumentXMLBytes || document.CompressedSize64 > MaxContentInputBytes {
		return TextContent{}, ErrContentTooLarge
	}
	reader, err := document.Open()
	if err != nil {
		return TextContent{}, ErrInvalidContent
	}
	defer reader.Close()
	// Read to EOF within the budget so CRC errors and dishonest declared sizes
	// are rejected. No archive path is ever passed to the operating system.
	xmlBytes, err := io.ReadAll(io.LimitReader(reader, maxDocumentXMLBytes+1))
	if len(xmlBytes) > maxDocumentXMLBytes {
		return TextContent{}, ErrContentTooLarge
	}
	if err != nil {
		return TextContent{}, ErrInvalidContent
	}
	return extractDocumentXML(ctx, xmlBytes)
}

func preflightZIP(data []byte) ([]byte, error) {
	const endHeaderSize = 22
	const directoryHeaderSize = 46
	if len(data) < endHeaderSize {
		return nil, ErrInvalidContent
	}
	minimum := len(data) - endHeaderSize - 0xffff
	if minimum < 0 {
		minimum = 0
	}
	end := -1
	for pos := len(data) - endHeaderSize; pos >= minimum; pos-- {
		if binary.LittleEndian.Uint32(data[pos:pos+4]) != 0x06054b50 {
			continue
		}
		commentLength := int(binary.LittleEndian.Uint16(data[pos+20 : pos+22]))
		if pos+endHeaderSize+commentLength == len(data) {
			end = pos
			break
		}
	}
	if end < 0 {
		return nil, ErrInvalidContent
	}
	header := data[end:]
	count := int(binary.LittleEndian.Uint16(header[10:12]))
	if count > maxZIPEntries {
		return nil, ErrContentTooLarge
	}
	if binary.LittleEndian.Uint16(header[4:6]) != 0 || binary.LittleEndian.Uint16(header[6:8]) != 0 || int(binary.LittleEndian.Uint16(header[8:10])) != count {
		return nil, ErrInvalidContent
	}
	size := uint64(binary.LittleEndian.Uint32(header[12:16]))
	offset := uint64(binary.LittleEndian.Uint32(header[16:20]))
	// ZIP64/split archives are outside this bounded DOCX reader. At our input
	// size budget a standard single-disk central directory is sufficient.
	if size == 0xffff || size == 0xffffffff || offset == 0xffffffff {
		return nil, ErrContentTooLarge
	}
	if offset > uint64(end) || size != uint64(end)-offset {
		return nil, ErrInvalidContent
	}
	directory := data[int(offset):int(offset+size)]
	actualCount := 0
	for len(directory) > 0 {
		if len(directory) < directoryHeaderSize || binary.LittleEndian.Uint32(directory[:4]) != 0x02014b50 {
			return nil, ErrInvalidContent
		}
		actualCount++
		if actualCount > maxZIPEntries {
			return nil, ErrContentTooLarge
		}
		length := directoryHeaderSize + int(binary.LittleEndian.Uint16(directory[28:30])) + int(binary.LittleEndian.Uint16(directory[30:32])) + int(binary.LittleEndian.Uint16(directory[32:34]))
		if length > len(directory) {
			return nil, ErrInvalidContent
		}
		directory = directory[length:]
	}
	if actualCount != count {
		return nil, ErrInvalidContent
	}
	if end+endHeaderSize < len(data) {
		// Ignore the ZIP comment, which cannot contribute document content.
		// A bounded copy with its length cleared also prevents a forged EOCD
		// marker inside that comment from redirecting archive/zip's search.
		normalized := append([]byte(nil), data[:end+endHeaderSize]...)
		normalized[end+20] = 0
		normalized[end+21] = 0
		return normalized, nil
	}
	return data, nil
}

func wordNamespace(value string) bool {
	return value == "http://schemas.openxmlformats.org/wordprocessingml/2006/main" || value == "http://purl.oclc.org/ooxml/wordprocessingml/main"
}

func extractDocumentXML(ctx context.Context, data []byte) (TextContent, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Strict = true
	var output boundedTextWriter
	var stack []xml.Name
	seenRoot := false
	for count := 0; ; count++ {
		if count >= maxDocumentTokens {
			return TextContent{}, ErrContentTooLarge
		}
		if err := ctx.Err(); err != nil {
			return TextContent{}, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			if !seenRoot || len(stack) != 0 {
				return TextContent{}, ErrInvalidContent
			}
			break
		}
		if err != nil {
			return TextContent{}, ErrInvalidContent
		}
		switch token := token.(type) {
		case xml.StartElement:
			if len(stack) >= maxDocumentDepth {
				return TextContent{}, ErrContentTooLarge
			}
			if len(stack) == 0 {
				if seenRoot || token.Name.Local != "document" || !wordNamespace(token.Name.Space) {
					return TextContent{}, ErrInvalidContent
				}
				seenRoot = true
			}
			stack = append(stack, token.Name)
			if wordNamespace(token.Name.Space) {
				switch token.Name.Local {
				case "tab":
					output.Write([]byte("\t"))
				case "br", "cr":
					output.Write([]byte("\n"))
				}
			}
		case xml.EndElement:
			if len(stack) == 0 {
				return TextContent{}, ErrInvalidContent
			}
			stack = stack[:len(stack)-1]
			if wordNamespace(token.Name.Space) && token.Name.Local == "p" {
				output.Write([]byte("\n"))
			}
		case xml.CharData:
			if len(stack) == 0 {
				if strings.TrimSpace(string(token)) != "" {
					return TextContent{}, ErrInvalidContent
				}
				continue
			}
			parent := stack[len(stack)-1]
			if parent.Local == "t" && wordNamespace(parent.Space) {
				output.Write(token)
			}
		case xml.Directive:
			return TextContent{}, ErrInvalidContent
		case xml.ProcInst:
			if token.Target != "xml" || seenRoot {
				return TextContent{}, ErrInvalidContent
			}
		}
	}
	return output.content("docx", true)
}

// boundedTextWriter discards excess output while continuing to report consumed
// bytes. Readers/processes can finish and validate their inputs without an
// unbounded allocation or a pipe-blocking early return.
type boundedTextWriter struct {
	data      []byte
	discarded bool
}

func (w *boundedTextWriter) Write(data []byte) (int, error) {
	keep := MaxTextBytes + utf8.UTFMax - len(w.data)
	if keep > len(data) {
		keep = len(data)
	}
	if keep > 0 {
		w.data = append(w.data, data[:keep]...)
	}
	if keep < len(data) {
		w.discarded = true
	}
	return len(data), nil
}

func (w *boundedTextWriter) content(format string, requireText bool) (TextContent, error) {
	text, truncated := truncateUTF8(w.data, MaxTextBytes)
	if !validText(text) {
		return TextContent{}, ErrInvalidContent
	}
	if requireText && len(bytes.TrimSpace(text)) == 0 {
		return TextContent{}, ErrNoText
	}
	return TextContent{Text: string(text), Format: format, Truncated: truncated || w.discarded}, nil
}

func extractPDF(ctx context.Context, data []byte) (TextContent, error) {
	if !bytes.HasPrefix(data, []byte("%PDF-")) {
		return TextContent{}, ErrInvalidContent
	}
	tool, err := exec.LookPath("pdftotext")
	if err != nil {
		return TextContent{}, ErrExtractorUnavailable
	}
	return extractPDFWithCommand(ctx, data, tool, exec.CommandContext)
}

func extractPDFWithCommand(ctx context.Context, data []byte, tool string, command func(context.Context, string, ...string) *exec.Cmd) (TextContent, error) {
	processCtx, cancel := context.WithTimeout(ctx, pdfTimeout)
	defer cancel()
	// Fixed arguments and bytes on stdin: neither filename nor document text can
	// become command-line options or a shell program. Output stays in memory.
	cmd := command(processCtx, tool, "-enc", "UTF-8", "-nopgbrk", "-", "-")
	cmd.Stdin = bytes.NewReader(data)
	var output boundedTextWriter
	cmd.Stdout = &output
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return TextContent{}, ctx.Err()
		}
		if processCtx.Err() != nil {
			return TextContent{}, ErrExtractionTimeout
		}
		return TextContent{}, ErrExtractionFailed
	}
	return output.content("pdf", true)
}
