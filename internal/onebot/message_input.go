package onebot

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

// prepareSend performs a pure validation pass before resolving external media.
// Mixed content is an ordered batch of native WeChat messages; the caller
// receives individual receipts, never a fabricated single native receipt.
func (r *Runtime) prepareSend(ctx context.Context, params map[string]json.RawMessage, snap bot.Snapshot, conversation string, group bool) (bot.Send, error) {
	req := bot.Send{Account: snap.Account, Epoch: snap.Epoch, Conversation: conversation}
	auto, err := paramBool(params, "auto_escape")
	if err != nil {
		return req, err
	}
	segments, err := parseMessage(params["message"], auto)
	if err != nil {
		return req, err
	}
	outputs := []bot.Output{}
	sources := map[int]string{}
	current := bot.Output{}
	flush := func() {
		if current.Text != "" || len(current.Mentions) > 0 {
			outputs = append(outputs, current)
			current = bot.Output{}
		}
	}
	for _, segment := range segments {
		switch segment.Type {
		case "text":
			current.Text += segment.Data["text"]
		case "at":
			if current.Text != "" {
				flush()
			}
			if !group {
				return req, errors.New("mention_requires_group")
			}
			id, e := strconv.ParseInt(segment.Data["qq"], 10, 64)
			if e != nil {
				return req, errors.New("invalid_mention")
			}
			ref, ok, e := r.State.resolveChecked(snap.Account, "user", id)
			if e != nil {
				return req, errStateUnavailable
			}
			if !ok {
				return req, errors.New("mention_unavailable")
			}
			if _, err := r.Channel.Member(snap.Account, conversation, ref.Key); err != nil {
				if errors.Is(err, bot.ErrStorage) || errors.Is(err, bot.ErrLimit) {
					return req, err
				}
				return req, errors.New("group_member_unavailable")
			}
			current.Mentions = append(current.Mentions, ref.Key)
		case "image", "file":
			flush()
			name := segment.Data["name"]
			if name == "" {
				name = "file.bin"
				if segment.Type == "image" {
					name = "image.png"
				}
			}
			if !validUploadName(name) {
				return req, errors.New("invalid_media_name")
			}
			source := segment.Data["file"]
			if source == "" {
				return req, errors.New("invalid_media_source")
			}
			sources[len(outputs)] = source
			outputs = append(outputs, bot.Output{Upload: &bot.Upload{Kind: segment.Type, Name: name}})
		default:
			return req, errors.New("unsupported_segment")
		}
	}
	flush()
	if len(outputs) == 0 || len(outputs) > 20 {
		return req, errors.New("invalid_message_parts")
	}
	for _, output := range outputs {
		if len(output.Text) > 4096 || len(output.Mentions) > 20 || (output.Upload == nil && strings.TrimSpace(output.Text) == "" && len(output.Mentions) == 0) {
			return req, errors.New("invalid_message")
		}
	}
	if raw, exists := params["wechat_request_id"]; exists {
		if json.Unmarshal(raw, &req.Key) != nil || req.Key == "" || len(req.Key) > 128 {
			return req, errors.New("invalid_wechat_request_id")
		}
	} else {
		random := make([]byte, 16)
		if _, err = rand.Read(random); err != nil {
			return req, errors.New("random_unavailable")
		}
		req.Key = hex.EncodeToString(random)
	}
	total := 0
	for index, output := range outputs {
		if output.Upload == nil {
			continue
		}
		source := sources[index]
		var data []byte
		if strings.HasPrefix(source, "base64://") {
			data, err = base64.StdEncoding.DecodeString(strings.TrimPrefix(source, "base64://"))
		} else if strings.HasPrefix(source, "stream://") {
			data, err = r.streamUploadBytes(ctx, snap.Account, source)
		} else {
			data, err = fetchMedia(ctx, source)
		}
		total += len(data)
		if errors.Is(err, errStreamStorage) || errors.Is(err, errStreamClosed) {
			return req, bot.ErrStorage
		}
		if err != nil || len(data) == 0 || total > 16<<20 {
			return req, errors.New("invalid_media")
		}
		outputs[index].Upload.Data = data
	}
	if len(outputs) == 1 {
		req.Text, req.Mentions, req.Upload = outputs[0].Text, outputs[0].Mentions, outputs[0].Upload
	} else {
		req.Outputs = outputs
	}
	return req, nil
}
