package media

import (
	"errors"
	"strings"
	"testing"
)

func TestQuotedReplyExtractsOnlyTheNewText(t *testing.T) {
	xml := `<msg><appmsg><type>57</type><title><![CDATA[新的引用回复]]></title><refermsg><type>3</type><content><![CDATA[<msg><img aeskey="SYNTHETIC_PRIVATE_KEY"/></msg>]]></content></refermsg></appmsg></msg>`
	text, err := QuotedReplyText([]byte(xml))
	if err != nil || text != "新的引用回复" || strings.Contains(text, "SYNTHETIC_PRIVATE_KEY") {
		t.Fatal("quoted media source escaped into the new text", err)
	}
	for _, xml := range []string{
		`<msg><appmsg><type>57</type><title>a</title><title>b</title></appmsg></msg>`,
		`<!DOCTYPE msg [<!ENTITY secret SYSTEM "file:///synthetic">]><msg><appmsg><type>57</type><title>&secret;</title></appmsg></msg>`,
		`<msg><appmsg><type>57</type><title></title></appmsg></msg>`,
	} {
		if text, err := QuotedReplyText([]byte(xml)); err == nil || text != "" {
			t.Fatal("invalid quoted reply was accepted")
		}
	}
	if _, err := QuotedReplyText([]byte(`<msg><appmsg><type>6</type><title>attachment.txt</title></appmsg></msg>`)); !errors.Is(err, ErrUnsupported) {
		t.Fatal("file appmsg became a quoted reply")
	}
}
