package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestGroupEnvelopePreservesMemberContentAndActualMentions(t *testing.T) {
	m := Message{Sender: []byte("room@chatroom"), Recipient: []byte("owner"), Content: []byte("wxid_bob:\n你好\nalice:\n保留第二行"), Source: []byte(`<msgsource><atuserlist><![CDATA[owner,wxid_carol]]></atuserlist></msgsource>`)}
	g, ok := m.Group([]byte("owner"))
	if !ok || string(g.Room) != "room@chatroom" || string(g.Member) != "wxid_bob" || string(g.Content) != "你好\nalice:\n保留第二行" || len(g.Mentions) != 2 {
		t.Fatal("group envelope incorrectly projected")
	}
	if string(m.Peer([]byte("owner"))) != "room@chatroom" || !bytes.Contains(m.Content, []byte("wxid_bob:")) {
		t.Fatal("projection mutated wire content")
	}
	m.Sender = []byte("owner")
	m.Recipient = []byte("room@chatroom")
	m.Content = []byte("hello")
	g, ok = m.Group([]byte("owner"))
	if !ok || string(g.Member) != "owner" {
		t.Fatal("own group send lost identity")
	}
	m.Recipient = []byte("peer")
	if _, ok = m.Group([]byte("owner")); ok {
		t.Fatal("private message misclassified")
	}
}

func TestMentionMetadataRejectsSpoofingAndMalformedXML(t *testing.T) {
	for _, bad := range []string{`<msgsource><atuserlist>owner</atuserlist><atuserlist>other</atuserlist></msgsource>`, `<msgsource><atuserlist><nested>owner</nested></atuserlist></msgsource>`, `<!DOCTYPE x [<!ENTITY p "owner">]><msgsource><atuserlist>&p;</atuserlist></msgsource>`, `<msgsource><atuserlist>owner</atuserlist></msgsource><extra/>`, `<msgsource><atuserlist xmlns="other">owner</atuserlist></msgsource>`, strings.Repeat("x", 65537)} {
		if _, err := ParseMentions([]byte(bad)); err == nil {
			t.Fatal("malformed mention metadata accepted")
		}
	}
	for _, body := range []string{"@owner 你好", `<msgsource><atuserlist>owner</atuserlist></msgsource>`} {
		m := Message{Sender: []byte("room@chatroom"), Recipient: []byte("owner"), Content: []byte("member:\n" + body)}
		g, _ := m.Group([]byte("owner"))
		if len(g.Mentions) != 0 {
			t.Fatal("content spoofed actual mention")
		}
	}
	source, err := MentionSource([]string{"owner", "wxid_bob"})
	if err != nil {
		t.Fatal(err)
	}
	members, err := ParseMentions([]byte(source))
	if err != nil || strings.Join(members, ",") != "owner,wxid_bob" {
		t.Fatal("mention source not parsable", err)
	}
}

func TestDecodeSyncRetainsSourceTagTenNotPushContent(t *testing.T) {
	source := []byte(`<msgsource><atuserlist>owner</atuserlist></msgsource>`)
	add := Join(Uint(1, 1), Blob(2, String([]byte("room@chatroom"))), Blob(3, String([]byte("owner"))), Uint(4, 1), Blob(5, String([]byte("bob:\nhi"))), Uint(6, 0), Uint(7, 0), Blob(8, Buffer(nil)), Uint(9, 123), Blob(10, source), Blob(11, []byte("push content")), Uint(12, 55))
	item := Join(Uint(1, 5), Blob(2, Buffer(add)))
	response := Join(Uint(1, 0), Blob(2, Join(Uint(1, 1), Blob(2, item))), Uint(3, 0), Blob(4, Buffer(Uint(1, 0))))
	r, err := DecodeSync(response)
	if err != nil || len(r.Messages) != 1 || !bytes.Equal(r.Messages[0].Source, source) {
		t.Fatal("native AddMsg tag10 not retained", err)
	}
}
