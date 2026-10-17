package service

import (
	"github.com/JFSAS/wispcat/internal/media"
	"github.com/JFSAS/wispcat/internal/protocol"
)

// messageView is the display boundary. Raw media XML carries download keys
// and must never be returned as chat text or passed to a model.
func messageView(message protocol.Message) (map[string]any, bool) {
	message = normalizedMessage(message, nil)
	if message.Type == 1 {
		return map[string]any{"kind": "text", "text": string(message.Content)}, true
	}
	if message.Type == 49 {
		if text, err := media.QuotedReplyText(message.Content); err == nil {
			return map[string]any{"kind": "text", "text": "[引用回复] " + text, "quoted_reply": true}, true
		}
	}
	description, err := media.Parse(message.Type, message.Content)
	if err != nil {
		return nil, false
	}
	metadata := description.Public()
	label := "[图片]"
	if metadata.Kind == media.File {
		label = "[文件] " + metadata.Name
	}
	return map[string]any{"kind": string(metadata.Kind), "text": label, "media": metadata}, true
}
