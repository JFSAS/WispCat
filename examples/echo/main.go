// A separate bot process. This example does not own a WeChat session or model.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/JFSAS/wispcat/pkg/client"
)

func main() {
	c, err := client.New(os.Getenv("WISPCAT_ONEBOT_ENDPOINT"), os.Getenv("WISPCAT_ONEBOT_TOKEN"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err = c.Events(ctx, func(raw json.RawMessage) error {
		var e struct {
			Post  string      `json:"post_type"`
			Kind  string      `json:"message_type"`
			User  json.Number `json:"user_id"`
			Group json.Number `json:"group_id"`
			ID    string      `json:"wechat_event_id"`
			Text  string      `json:"raw_message"`
		}
		if json.Unmarshal(raw, &e) != nil || e.Post != "message" || e.Text != "/ping" {
			return nil
		}
		params := map[string]any{"message": []client.Segment{client.Text("pong")}, "wechat_request_id": "echo/" + e.ID}
		action := "send_private_msg"
		if e.Kind == "group" {
			action = "send_group_msg"
			params["group_id"] = e.Group
		} else {
			params["user_id"] = e.User
		}
		_, err := c.Call(ctx, action, params)
		return err
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "bot disconnected:", err)
		os.Exit(1)
	}
}
