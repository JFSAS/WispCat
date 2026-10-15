package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

type Segment struct {
	Type string            `json:"type"`
	Data map[string]string `json:"data"`
}

func Text(text string) Segment { return Segment{Type: "text", Data: map[string]string{"text": text}} }
func At(user string) Segment   { return Segment{Type: "at", Data: map[string]string{"qq": user}} }
func Image(data []byte) Segment {
	return Segment{Type: "image", Data: map[string]string{"file": "base64://" + base64.StdEncoding.EncodeToString(data)}}
}

// Events consumes a forward Event socket until cancellation or disconnect.
// The caller owns its restart/replay policy; OneBot has no processing ACK.
func (c *Client) Events(ctx context.Context, consume func(json.RawMessage) error) error {
	u := *c.base
	u.Scheme = map[string]string{"http": "ws", "https": "wss"}[u.Scheme]
	u.Path = strings.TrimRight(u.Path, "/") + "/event"
	connection, response, err := websocket.DefaultDialer.DialContext(ctx, u.String(), http.Header{"Authorization": []string{"Bearer " + c.token}})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return err
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	connection.SetReadLimit(24 << 20)
	for {
		_, raw, err := connection.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err = consume(json.RawMessage(raw)); err != nil {
			return err
		}
	}
}
