package protocol

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

func IsGroup(username []byte) bool {
	return len(username) > len("@chatroom") && bytes.HasSuffix(username, []byte("@chatroom"))
}

// GroupEnvelope is a projection of the saved wire message. Source and account
// identifiers remain private; the service maps them to local opaque IDs.
type GroupEnvelope struct {
	Room     []byte
	Member   []byte
	Content  []byte
	Mentions []string
}

func (g GroupEnvelope) String() string   { return "protocol.GroupEnvelope(redacted)" }
func (g GroupEnvelope) GoString() string { return g.String() }

func validMember(value string) bool {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, ",:<>&/\\ \t\r\n\x00") || strings.HasSuffix(value, "@chatroom") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

// Group projects incoming member:\ncontent without guessing from visible
// nicknames. Malformed/missing member prefixes retain content but no sender.
func (m Message) Group(owner []byte) (GroupEnvelope, bool) {
	var g GroupEnvelope
	if IsGroup(m.Sender) {
		g.Room = bytes.Clone(m.Sender)
	} else if IsGroup(m.Recipient) {
		g.Room = bytes.Clone(m.Recipient)
	} else {
		return g, false
	}
	g.Content = bytes.Clone(m.Content)
	if bytes.Equal(m.Sender, owner) {
		g.Member = bytes.Clone(owner)
	} else if IsGroup(m.Sender) {
		if offset := bytes.Index(m.Content, []byte(":\n")); offset > 0 && offset <= 512 && validMember(string(m.Content[:offset])) {
			g.Member = bytes.Clone(m.Content[:offset])
			g.Content = bytes.Clone(m.Content[offset+2:])
		}
	}
	g.Mentions, _ = ParseMentions(m.Source)
	return g, true
}

// ParseMentions accepts only bounded msgsource XML's direct atuserlist. Text
// that merely looks like @nickname or quoted XML in Content is not a mention.
func ParseMentions(source []byte) ([]string, error) {
	if len(source) == 0 {
		return nil, nil
	}
	if len(source) > 64<<10 || !utf8.Valid(source) {
		return nil, ErrWire
	}
	d := xml.NewDecoder(bytes.NewReader(source))
	var stack []string
	var list string
	roots, count, tokens := 0, 0, 0
	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ErrWire
		}
		tokens++
		if tokens > 4096 {
			return nil, ErrWire
		}
		switch v := t.(type) {
		case xml.StartElement:
			if v.Name.Space != "" || len(stack) >= 16 {
				return nil, ErrWire
			}
			if len(stack) == 0 {
				roots++
				if roots != 1 || v.Name.Local != "msgsource" {
					return nil, ErrWire
				}
			}
			if v.Name.Local == "atuserlist" {
				if len(stack) != 1 || stack[0] != "msgsource" || count != 0 {
					return nil, ErrWire
				}
				var text strings.Builder
				for {
					child, err := d.Token()
					if err != nil {
						return nil, ErrWire
					}
					switch inner := child.(type) {
					case xml.CharData:
						text.Write(inner)
					case xml.EndElement:
						if inner.Name != v.Name {
							return nil, ErrWire
						}
						list = text.String()
						goto listRead
					default:
						return nil, ErrWire
					}
				}
			listRead:
				count++
				continue
			}
			stack = append(stack, v.Name.Local)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, ErrWire
			}
			stack = stack[:len(stack)-1]
		case xml.Directive:
			return nil, ErrWire
		case xml.CharData:
			if len(stack) == 0 && strings.TrimSpace(string(v)) != "" {
				return nil, ErrWire
			}
		}
	}
	if roots != 1 || len(stack) != 0 {
		return nil, ErrWire
	}
	if count == 0 || strings.TrimSpace(list) == "" {
		return nil, nil
	}
	items := strings.Split(list, ",")
	if len(items) > 100 {
		return nil, ErrWire
	}
	seen := map[string]bool{}
	result := []string{}
	for _, item := range items {
		value := strings.TrimSpace(item)
		if !validMember(value) {
			return nil, ErrWire
		}
		if !seen[value] {
			result = append(result, value)
			seen[value] = true
		}
	}
	return result, nil
}

func MentionSource(members []string) (string, error) {
	if len(members) == 0 {
		return "", nil
	}
	if len(members) > 100 {
		return "", ErrWire
	}
	seen := map[string]bool{}
	for _, member := range members {
		if !validMember(member) || seen[member] {
			return "", ErrWire
		}
		seen[member] = true
	}
	value := struct {
		XMLName xml.Name `xml:"msgsource"`
		Members string   `xml:"atuserlist"`
	}{Members: strings.Join(members, ",")}
	encoded, err := xml.Marshal(value)
	if err != nil {
		return "", errors.New("mention metadata encoding failed")
	}
	return string(encoded), nil
}
