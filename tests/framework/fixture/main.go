//go:build integrationfixtures

// This executable can only create synthetic channels. It has no login, account
// import or protocol worker and is not part of the production runtime binary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

const fixtureToken = "synthetic-framework-token"

type synthetic struct {
	mu         sync.Mutex
	account    string
	cursor     uint64
	events     []bot.Event
	sends      []bot.Send
	deliveries map[string]bot.Delivery
	keys       map[string]string
}

func (c *synthetic) Snapshot() bot.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bot.Snapshot{Account: c.account, Self: bot.User{ID: "owner", Name: "Synthetic Bot"}, Online: true, State: "connected", Epoch: 1, Cursor: c.cursor}
}
func (c *synthetic) Users() []bot.User {
	return []bot.User{{ID: "owner", Name: "Synthetic Bot"}, {ID: "friend", Name: "Synthetic Friend"}}
}
func (c *synthetic) Members(account, conversation string) (bot.Roster, error) {
	if account != c.Snapshot().Account {
		return bot.Roster{}, bot.ErrScope
	}
	if conversation != "room" {
		return bot.Roster{}, bot.ErrNotFound
	}
	return bot.Roster{Users: c.Users(), Source: "synthetic_observed", Complete: false}, nil
}
func (c *synthetic) Conversations() []bot.Conversation {
	return []bot.Conversation{{ID: "friend", Kind: "private", Name: "Synthetic Friend"}, {ID: "room", Kind: "group", Name: "Synthetic Room"}}
}
func (c *synthetic) Events(account string, after uint64, limit int) ([]bot.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if account != c.account {
		return nil, bot.ErrScope
	}
	if after > c.cursor {
		return nil, bot.ErrCursor
	}
	result := []bot.Event{}
	for _, e := range c.events {
		if e.Sequence > after {
			result = append(result, e)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}
func (c *synthetic) Message(account, id string) (bot.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.events {
		if e.Account == account && e.ID == id {
			return e, nil
		}
	}
	return bot.Event{}, bot.ErrNotFound
}
func (c *synthetic) Submit(ctx context.Context, q bot.Send) (bot.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return bot.Delivery{}, err
	}
	if q.Account != c.account || q.Epoch != 1 {
		return bot.Delivery{}, bot.ErrScope
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	raw, _ := json.Marshal(q)
	if previous, ok := c.keys[q.Key]; ok {
		if previous != string(raw) {
			return bot.Delivery{}, bot.ErrConflict
		}
		return c.deliveries[q.Key], nil
	}
	c.keys[q.Key] = string(raw)
	c.sends = append(c.sends, q)
	d := bot.Delivery{ID: q.Key, MessageID: "receipt-" + strconv.Itoa(len(c.sends)), State: "accepted", Conversation: q.Conversation, Text: q.Text, Time: time.Now().Unix()}
	c.deliveries[q.Key] = d
	return d, nil
}
func (c *synthetic) Delivery(account, id string) (bot.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if account != c.account {
		return bot.Delivery{}, bot.ErrScope
	}
	d, ok := c.deliveries[id]
	if !ok {
		return d, bot.ErrNotFound
	}
	return d, nil
}
func (c *synthetic) Download(context.Context, string, string, string) (bot.Content, error) {
	return bot.Content{}, bot.ErrUnsupported
}
func (c *synthetic) inject(kind, text string, mentioned bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor++
	conversation := bot.Conversation{ID: "friend", Kind: "private", Name: "Synthetic Friend"}
	if kind == "group" {
		conversation = bot.Conversation{ID: "room", Kind: "group", Name: "Synthetic Room"}
	}
	segments := []bot.Segment{}
	if mentioned {
		segments = append(segments, bot.Segment{Type: "at", Data: map[string]string{"user_id": "owner"}})
	}
	segments = append(segments, bot.Segment{Type: "text", Data: map[string]string{"text": text}})
	c.events = append(c.events, bot.Event{ID: "event-" + strconv.FormatUint(c.cursor, 10), Sequence: c.cursor, Account: c.account, Conversation: conversation, Sender: bot.User{ID: "friend", Name: "Synthetic Friend"}, Time: time.Now().Unix(), Message: segments})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	reverse := flag.String("reverse", "", "loopback framework endpoint")
	stateDir := flag.String("state-dir", "", "synthetic fixture state")
	flag.Parse()
	u, err := url.Parse(*reverse)
	if err != nil || u.Scheme != "ws" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() || *stateDir == "" {
		return errors.New("fixture_requires_loopback_and_private_state")
	}
	state, err := onebot.OpenState(*stateDir)
	if err != nil {
		return err
	}
	channels := []*synthetic{}
	servers := []*onebot.Server{}
	for _, account := range []string{"synthetic-account-a", "synthetic-account-b"} {
		channel := &synthetic{account: account, deliveries: map[string]bot.Delivery{}, keys: map[string]string{}}
		channels = append(channels, channel)
		servers = append(servers, onebot.NewServer(onebot.New(channel, state), fixtureToken, ""))
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+fixtureToken {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/status":
			connections := []onebot.ConnectionStatus{}
			for _, server := range servers {
				connections = append(connections, server.Statuses()...)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"synthetic": true, "connections": connections})
		case "/inject":
			var q struct {
				Account   string `json:"account"`
				Kind      string `json:"kind"`
				Text      string `json:"text"`
				Mentioned bool   `json:"mentioned"`
			}
			r.Body = http.MaxBytesReader(w, r.Body, 4096)
			if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&q) != nil || (q.Kind != "private" && q.Kind != "group") {
				w.WriteHeader(400)
				return
			}
			found := false
			for _, channel := range channels {
				if channel.account == q.Account {
					channel.inject(q.Kind, q.Text, q.Mentioned)
					found = true
				}
			}
			if !found {
				w.WriteHeader(400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"injected": true})
		case "/sends":
			sends := []bot.Send{}
			for _, channel := range channels {
				channel.mu.Lock()
				sends = append(sends, channel.sends...)
				channel.mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(sends)
		default:
			w.WriteHeader(404)
		}
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second}
	go httpServer.Serve(listener)
	done := make(chan error, len(servers))
	for _, server := range servers {
		go func(server *onebot.Server) {
			done <- server.RunReverse(ctx, onebot.ReverseConfig{Name: "framework", URL: u.String(), Token: fixtureToken})
		}(server)
	}
	fmt.Println("control: http://" + listener.Addr().String())
	<-ctx.Done()
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	_ = httpServer.Shutdown(shutdown)
	for _, server := range servers {
		if err = server.Close(shutdown); err != nil {
			return err
		}
	}
	for range servers {
		select {
		case err = <-done:
			if err != nil {
				return err
			}
		case <-shutdown.Done():
			return shutdown.Err()
		}
	}
	return nil
}

var _ bot.Channel = (*synthetic)(nil)
