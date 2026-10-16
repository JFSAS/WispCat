//go:build integrationfixtures

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func deploymentMediaService(t *testing.T, directory string) (BotAdapter, *DeploymentDriver, func()) {
	t.Helper()
	s := New(directory, time.Second, "instance:deployment-synthetic-media")
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(directory)
	if err != nil {
		t.Fatal(err)
	}
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	d, err := NewDeploymentDriver(s, "synthetic-media")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx, "offline")
	return a, d, func() {
		cancel()
		d.Wait()
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}
}

func TestDeploymentMediaWorkersPreserveBytesEncodeAndReopen(t *testing.T) {
	for _, kind := range []string{"image", "file"} {
		for _, conversation := range []string{"private", "group"} {
			t.Run(kind+"/"+conversation, func(t *testing.T) {
				root := t.TempDir()
				a, d, closeFirst := deploymentMediaService(t, root)
				if err := d.InjectMedia(conversation, kind); err != nil {
					closeFirst()
					t.Fatal(err)
				}
				snap := a.Snapshot()
				events, err := a.Events(snap.Account, 0, 10)
				if err != nil || len(events) != 1 {
					closeFirst()
					t.Fatal(err, len(events))
				}
				event := events[0]
				reference := ""
				for _, part := range event.Message {
					if part.Type == kind {
						reference = part.Data["file"]
					}
				}
				body, name, err := deploymentMediaPayload(kind, "synthetic-media")
				if err != nil {
					closeFirst()
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				content, err := a.Download(ctx, snap.Account, event.Conversation.ID, reference)
				if err != nil || !bytes.Equal(content.Data, body) {
					closeFirst()
					t.Fatal("native download bytes", err)
				}
				result, err := a.Submit(ctx, bot.Send{Account: snap.Account, Epoch: snap.Epoch, Conversation: event.Conversation.ID, Key: "deployment-media-reply", Upload: &bot.Upload{Kind: kind, Name: name, Data: body}})
				if err != nil {
					closeFirst()
					t.Fatal(err)
				}
				deadline := time.Now().Add(3 * time.Second)
				for len(d.Inspect().Sends) == 0 && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				state := d.Inspect()
				sum := sha256.Sum256(body)
				if state.Error != "" || len(state.Sends) != 1 || state.Uploads != 1 || state.Downloads != 1 {
					closeFirst()
					t.Fatal("native media fixture did not settle", state.Error, len(state.Sends), state.Uploads, state.Downloads)
				}
				row := state.Sends[0]
				if row.ID != result.ID || row.Media != kind || row.Kind != conversation || row.PayloadSHA != hex.EncodeToString(sum[:]) || row.EncodedBytes == 0 || len(row.EncodedSHA) != 64 {
					closeFirst()
					t.Fatal("media/codec observation mismatch")
				}
				if a.Service.calls["send"] != 1 || a.Service.calls["auth"] != 0 || a.Service.calls["sync"] != 0 {
					closeFirst()
					t.Fatal("unexpected worker/session counters")
				}
				closeFirst()
				next, d2, closeNext := deploymentMediaService(t, root)
				defer closeNext()
				cached, err := next.Download(ctx, snap.Account, event.Conversation.ID, reference)
				if err != nil || !bytes.Equal(cached.Data, body) || d2.Inspect().Downloads != 0 {
					t.Fatal("cached incoming bytes changed after reopen", err)
				}
				receipt, err := next.Delivery(snap.Account, result.ID)
				if err != nil || receipt.State != "accepted" {
					t.Fatal("outgoing receipt did not survive", receipt.State, err)
				}
			})
		}
	}
}

func TestDeploymentMediaDownloaderRejectsUnregisteredDescriptor(t *testing.T) {
	_, d, closeFixture := deploymentMediaService(t, t.TempDir())
	defer closeFixture()
	for _, kind := range []string{"image", "file"} {
		if err := d.InjectMedia("private", kind); err != nil {
			t.Fatal(err)
		}
		s := d.service
		s.mu.RLock()
		message := s.data.Messages[len(s.data.Messages)-1]
		s.mu.RUnlock()
		xml := strings.ReplaceAll(string(message.Content), "fixture-"+kind+"-synthetic-media", "unregistered")
		description, err := media.Parse(message.Type, []byte(xml))
		if err != nil {
			t.Fatal(err)
		}
		variant := media.Regular
		if kind == "file" {
			variant = media.Attachment
		}
		if _, err = d.downloadMedia(context.Background(), MediaDownloadRequest{Variant: variant, Description: description, MaxBytes: 16 << 20}); err == nil {
			t.Fatal("unregistered source accepted")
		}
	}
	if d.Inspect().Downloads != 0 {
		t.Fatal("unknown fixture reference was resolved")
	}
}
