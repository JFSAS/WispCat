package service

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func mediaFixture(t *testing.T, body []byte, kind, filename string) (*Service, string, string) {
	t.Helper()
	s := New(t.TempDir(), time.Second, "/synthetic/WeChat.app")
	bootstrap := protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 1234, DeviceID: []byte("synthetic-device")}
	s.data = &store.Data{Schema: 1, Binding: bootstrap.Binding(), APIKey: bytes.Repeat([]byte{1}, 32), NextClientID: 1, Jobs: map[string]*store.Job{}, Session: &protocol.Session{Bootstrap: bootstrap, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24)}}
	s.client = &protocol.Client{Bootstrap: bootstrap, State: cloneSession(s.data.Session)}
	s.checkpoint = store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: s.data.Binding}
	s.state, s.mode = "connected", "online"
	hash := md5.Sum(body)
	message := protocol.Message{Sender: bootstrap.Username, Recipient: []byte("filehelper"), Timestamp: 101, ServerID: 55, Type: 3}
	if kind == "image" {
		message.Content = []byte(fmt.Sprintf(`<msg><img aeskey="00112233445566778899aabbccddeeff" cdnmidimgurl="synthetic-private-CDN-id" length="%d" md5="%x" cdnthumbaeskey="ffeeddccbbaa99887766554433221100" cdnthumburl="synthetic-private-thumbnail-id" cdnthumblength="%d"/></msg>`, len(body), hash, len(body)))
	} else {
		message.Type = 49
		message.Content = []byte(fmt.Sprintf(`<msg><appmsg><title>%s</title><type>6</type><appattach><totallen>%d</totallen><aeskey>00112233445566778899aabbccddeeff</aeskey><cdnattachurl>synthetic-private-CDN-id</cdnattachurl><md5>%x</md5></appattach></appmsg></msg>`, filename, len(body), hash))
	}
	s.data.Messages = []protocol.Message{message}
	return s, s.sessionID([]byte("filehelper")), messageKey(message, bootstrap.Username)
}

func mediaTestPNG(t *testing.T) []byte {
	t.Helper()
	var output bytes.Buffer
	if err := png.Encode(&output, image.NewRGBA(image.Rect(0, 0, 3, 2))); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func mediaQueueID(t *testing.T, s *Service, session, message, variant string) string {
	t.Helper()
	result, err := s.QueueMediaDownload(session, message, variant)
	if err != nil {
		t.Fatal(err)
	}
	return result["id"].(string)
}

func TestFailedMediaCanBeExplicitlyRetriedWithoutDuplicatingTheTask(t *testing.T) {
	body := mediaTestPNG(t)
	s, session, message := mediaFixture(t, body, "image", "")
	calls := 0
	s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
		calls++
		if calls == 1 {
			return nil, &cdn.Error{Category: "network_error"}
		}
		return verifiedMedia(body), nil
	})
	id := mediaQueueID(t, s, session, message, "regular")
	if err := s.downloadQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.data.MediaJobs[id].Status != "failed" {
		t.Fatal("failed download was not recorded")
	}
	if second := mediaQueueID(t, s, session, message, "regular"); second != id || s.data.MediaJobs[id].Status != "queued" {
		t.Fatal("explicit retry returned the old failure instead of queueing the download")
	}
	mediaQueueID(t, s, session, message, "regular")
	if err := s.downloadQueued(context.Background()); err != nil || calls != 2 || len(s.MediaJobs()) != 1 || s.data.MediaJobs[id].Status != "downloaded" {
		t.Fatal("explicit retry duplicated a task or did not download the recovered image", err)
	}
}

func TestMediaDecoderFailuresRetainSafeCategories(t *testing.T) {
	category, _ := mediaFailure(&cdn.Error{Category: "cipher_size_mismatch"})
	if category != "cdn_cipher_size_mismatch" {
		t.Fatal("specific CDN decoder failure was hidden behind media_download_failed", category)
	}
	category, _ = mediaFailure(&cdn.Error{Category: "SYNTHETIC_PRIVATE_KEY"})
	if category != "media_download_failed" {
		t.Fatal("unknown CDN error leaked private text", category)
	}
}

func verifiedMedia(body []byte) *cdn.Result {
	return &cdn.Result{Data: bytes.Clone(body), RawSize: uint64(len(body)), ChecksumVerified: true}
}

func TestMediaDefaultUnknownAndBadDecodeNeverBecomeDownloaded(t *testing.T) {
	body := mediaTestPNG(t)
	for _, scenario := range []string{"unconfigured", "nil-result", "wrong-size", "wrong-hash", "private-error", "server-error"} {
		t.Run(scenario, func(t *testing.T) {
			s, session, message := mediaFixture(t, body, "image", "")
			calls := 0
			if scenario != "unconfigured" {
				s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
					calls++
					switch scenario {
					case "nil-result":
						return nil, nil
					case "wrong-size":
						result := verifiedMedia(body)
						result.RawSize++
						return result, nil
					case "wrong-hash":
						return verifiedMedia([]byte("not matching image")), nil
					case "private-error":
						return nil, errors.New("synthetic_private_ticket")
					case "server-error":
						return nil, &cdn.Error{Category: "server_error", Code: -5103009}
					default:
						return nil, nil
					}
				})
			}
			id := mediaQueueID(t, s, session, message, "regular")
			if err := s.downloadQueued(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.downloadQueued(context.Background()); err != nil || calls > 1 {
				t.Fatal("failure auto-retried", err)
			}
			jobs := s.MediaJobs()
			if jobs[0]["status"] != "failed" {
				t.Fatal("unknown/invalid download claims success")
			}
			encoded, _ := json.Marshal(jobs)
			if strings.Contains(string(encoded), "synthetic_private_ticket") {
				t.Fatal("downloader error leaked")
			}
			if _, err := s.MediaContent(id); !errors.Is(err, ErrMediaNotReady) {
				t.Fatal("failed bytes were served", err)
			}
			if _, err := os.Stat(filepath.Join(s.directory, "media", id+".enc")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unverified plaintext was cached", err)
			}
		})
	}
}

func TestMediaPauseAndBindingChangeDiscardCompletedBytes(t *testing.T) {
	body := mediaTestPNG(t)
	for _, scenario := range []string{"offline-before", "pause-during", "scope-change"} {
		t.Run(scenario, func(t *testing.T) {
			s, session, message := mediaFixture(t, body, "image", "")
			calls := 0
			s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
				calls++
				if scenario == "pause-during" {
					s.SetMode("offline", false)
				}
				if scenario == "scope-change" {
					s.mu.Lock()
					copy := *s.data
					sessionCopy := *s.data.Session
					sessionCopy.Bootstrap.Username = []byte("different-synthetic-owner")
					copy.Session = &sessionCopy
					copy.Binding = sessionCopy.Bootstrap.Binding()
					copy.MediaJobs = nil
					s.data = &copy
					s.mu.Unlock()
				}
				return verifiedMedia(body), nil
			})
			id := mediaQueueID(t, s, session, message, "regular")
			if scenario == "offline-before" {
				s.SetMode("offline", false)
			}
			if err := s.downloadQueued(context.Background()); err != nil {
				t.Fatal(err)
			}
			if scenario == "offline-before" && calls != 0 {
				t.Fatal("offline mode performed network operation")
			}
			if _, err := os.Stat(filepath.Join(s.directory, "media", id+".enc")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("paused or old-account bytes were retained", err)
			}
			if _, err := s.MediaContent(id); err == nil {
				t.Fatal("paused or old-account bytes were exposed")
			}
			if scenario != "scope-change" && s.MediaJobs()[0]["status"] != "queued" {
				t.Fatal("paused work lost its queued state")
			}
		})
	}
}

func TestMediaQueueValidatesSessionMessageVariantAndScope(t *testing.T) {
	s, session, message := mediaFixture(t, mediaTestPNG(t), "image", "")
	for _, request := range [][3]string{{"bad", message, "regular"}, {strings.Repeat("a", 32), message, "regular"}, {session, strings.Repeat("a", 64), "regular"}, {session, message, "attachment"}, {session, message, "original"}, {session, message, "http://invalid.test"}} {
		if _, err := s.QueueMediaDownload(request[0], request[1], request[2]); err == nil {
			t.Fatal("invalid media target accepted")
		}
	}
	if len(s.MediaJobs()) != 0 {
		t.Fatal("invalid requests created work")
	}
	id := mediaQueueID(t, s, session, message, "regular")
	s.mu.Lock()
	s.data.Session.Bootstrap.Username = []byte("different-owner-with-stale-binding")
	s.mu.Unlock()
	if len(s.MediaJobs()) != 0 {
		t.Fatal("inconsistent account binding exposes jobs")
	}
	if _, err := s.MediaContent(id); !errors.Is(err, ErrMediaUnavailable) {
		t.Fatal("inconsistent account binding exposes content", err)
	}
}
