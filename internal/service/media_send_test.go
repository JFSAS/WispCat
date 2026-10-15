package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func TestMediaSendJournalAndNonReplayableBoundary(t *testing.T) {
	for _, kind := range []string{"image", "file"} {
		t.Run(kind, func(t *testing.T) {
			body := mediaTestPNG(t)
			name := "sample.png"
			if kind == "file" {
				body = []byte("test: synthetic\n")
				name = "sample.yaml"
			}
			s, session, _ := mediaFixture(t, mediaTestPNG(t), "image", "")
			view, err := s.QueueMediaSend(session, kind, name, body)
			if err != nil {
				t.Fatal(err)
			}
			id := view["id"].(string)
			loaded, err := s.checkpoint.Load()
			if err != nil || loaded.Jobs[id].Media.NativeID == "" {
				t.Fatal("missing durable identity", err)
			}
			journal, err := os.ReadFile(filepath.Join(s.directory, "checkpoints", s.data.Binding+".checkpoint"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(journal, body) {
				t.Fatal("journal contains plaintext attachment")
			}
			cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: s.data.Binding}
			cached, err := cache.Load(id)
			if err != nil || !bytes.Equal(cached, body) {
				t.Fatal("cache corrupt", err)
			}
			calls := 0
			s.SetMediaSender(func(ctx context.Context, r MediaSendRequest) (MediaSendResult, error) {
				calls++
				if r.Peer != "filehelper" || r.Kind != kind || !bytes.Equal(r.Data, body) || r.Metadata.NativeID != loaded.Jobs[id].Media.NativeID {
					t.Error("wrong send input")
				}
				diagnostic, _ := json.Marshal(r)
				if bytes.Contains(diagnostic, body) || strings.Contains(string(diagnostic), "filehelper") || strings.Contains(string(diagnostic), r.Metadata.NativeID) {
					t.Error("private send input leaked")
				}
				progress := r.Metadata
				progress.Stage = "uploaded"
				progress.FileID = "synthetic-secret-reference"
				progress.MD5 = strings.Repeat("a", 32)
				progress.UploadSize = uint32(len(body))
				if err := r.SaveProgress(progress); err != nil {
					return MediaSendResult{}, err
				}
				progress.Stage = "submitting"
				if err := r.SaveProgress(progress); err != nil {
					return MediaSendResult{}, err
				}
				persisted, err := s.checkpoint.Load()
				if err != nil || persisted.Jobs[id].Status != "unknown" || persisted.Jobs[id].Media.FileID != progress.FileID {
					t.Error("submission boundary not durable", err)
				}
				return MediaSendResult{Receipt: protocol.MediaSubmissionReceipt{ServerAccepted: true, ServerID: 9001}, SubmissionStarted: true}, nil
			})
			if err := s.sendQueued(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.sendQueued(context.Background()); err != nil || calls != 1 {
				t.Fatal("accepted job was replayed", err)
			}
			if s.data.Jobs[id].Status != "accepted" || s.data.Jobs[id].ServerID != 9001 {
				t.Fatal("acceptance missing")
			}
			public, _ := json.Marshal(s.Jobs())
			messages, _ := s.Messages(session)
			publicMessages, _ := json.Marshal(messages)
			for _, out := range [][]byte{public, publicMessages} {
				if bytes.Contains(out, body) || bytes.Contains(out, []byte("synthetic-secret-reference")) || bytes.Contains(out, []byte(s.data.Jobs[id].Media.NativeID)) {
					t.Fatal("public state leaked attachment or ticket")
				}
			}
		})
	}
}

func TestMediaSendUnknownFailedAndPauseNeverReplay(t *testing.T) {
	for _, test := range []struct {
		name          string
		submit, pause bool
		reject        error
		want          string
	}{
		{"upload failure", false, false, errors.New("private-detail"), "failed"},
		{"lost acknowledgment", true, false, errors.New("private-detail"), "unknown"},
		{"explicit CGI reject", true, false, &protocol.Error{Category: "media_submit_rejected", Code: -4}, "failed"},
		{"pause after upload", false, true, nil, "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, session, _ := mediaFixture(t, mediaTestPNG(t), "image", "")
			view, err := s.QueueMediaSend(session, "file", "sample.yaml", []byte("test: synthetic\n"))
			if err != nil {
				t.Fatal(err)
			}
			id := view["id"].(string)
			calls := 0
			s.SetMediaSender(func(ctx context.Context, r MediaSendRequest) (MediaSendResult, error) {
				calls++
				progress := r.Metadata
				if test.pause {
					if err := s.SetMode("offline", false); err != nil {
						t.Fatal(err)
					}
					progress.Stage = "uploaded"
					return MediaSendResult{}, r.SaveProgress(progress)
				}
				if test.submit {
					progress.Stage = "submitting"
					if err := r.SaveProgress(progress); err != nil {
						return MediaSendResult{}, err
					}
				}
				return MediaSendResult{SubmissionStarted: test.submit}, test.reject
			})
			if err := s.sendQueued(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.data.Jobs[id].Status != test.want {
				t.Fatal(s.data.Jobs[id].Status)
			}
			if err := s.sendQueued(context.Background()); err != nil || calls != 1 {
				t.Fatal("terminal replay", err)
			}
			public, _ := json.Marshal(s.Jobs())
			if bytes.Contains(public, []byte("private-detail")) {
				t.Fatal("raw error leaked")
			}
		})
	}
}

func TestMediaSendRejectsInvalidInputAndTamperedCache(t *testing.T) {
	s, session, _ := mediaFixture(t, mediaTestPNG(t), "image", "")
	for _, item := range []struct {
		kind, name string
		data       []byte
	}{
		{"file", "../secret", []byte("test")}, {"image", "bad.png", []byte("not an image")}, {"file", "empty", nil}, {"file", "large", make([]byte, MaxMediaSendBytes+1)}, {"other", "test", []byte("test")},
	} {
		if _, err := s.QueueMediaSend(session, item.kind, item.name, item.data); err == nil {
			t.Fatal("invalid input queued")
		}
	}
	if len(s.data.Jobs) != 0 {
		t.Fatal("invalid input mutated journal")
	}
	view, err := s.QueueMediaSend(session, "file", "sample.yaml", []byte("test\n"))
	if err != nil {
		t.Fatal(err)
	}
	id := view["id"].(string)
	cachePath := filepath.Join(s.directory, "media", id+".enc")
	if err := os.WriteFile(cachePath, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.SetMediaSender(func(context.Context, MediaSendRequest) (MediaSendResult, error) {
		calls++
		return MediaSendResult{}, nil
	})
	if err := s.sendQueued(context.Background()); err != nil || calls != 0 || s.data.Jobs[id].Status != "failed" {
		t.Fatal("tampered bytes sent", err)
	}
}

func TestPreparedImageCreatesBoundedJPEGAndThumbnail(t *testing.T) {
	var source bytes.Buffer
	if err := png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 2500, 1000))); err != nil {
		t.Fatal(err)
	}
	main, thumb, w, h, tw, th, err := prepareImageUpload(source.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if w != 2048 || h != 819 || tw != 320 || th != 128 {
		t.Fatal("incorrect proportional dimensions", w, h, tw, th)
	}
	for _, data := range [][]byte{main, thumb} {
		info, err := media.ValidateImage(data)
		if err != nil || info.MIME != "image/jpeg" {
			t.Fatal("not JPEG", err)
		}
	}
	decoded, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatal(err)
	}
	r, g, b, a := decoded.At(0, 0).RGBA()
	if r < 65000 || g < 65000 || b < 65000 || a != 65535 {
		t.Fatal("transparent background not white")
	}
}
