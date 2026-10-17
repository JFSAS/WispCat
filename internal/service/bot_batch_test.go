package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JFSAS/wispcat/internal/store"
	"github.com/JFSAS/wispcat/pkg/bot"
)

func mixedFixture(t *testing.T) (BotAdapter, bot.Send) {
	t.Helper()
	s := recoveryFixture(t)
	s.data.NextClientID = 1
	a := BotAdapter{Service: s}
	snap := a.Snapshot()
	return a, bot.Send{Account: snap.Account, Epoch: snap.Epoch, Conversation: s.sessionID([]byte("filehelper")), Key: "mixed", Outputs: []bot.Output{{Text: "before"}, {Upload: &bot.Upload{Kind: "image", Name: "image.png", Data: mediaTestPNG(t)}}, {Upload: &bot.Upload{Kind: "file", Name: "note.txt", Data: []byte("synthetic file")}}, {Text: "after"}}}
}

func TestBatchCounterWrapPreservesNativeDispatchOrder(t *testing.T) {
	a, req := mixedFixture(t)
	a.Service.data.NextClientID = 0x7fffffff
	d, err := a.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var previous uint32
	for _, part := range d.Parts {
		job := a.Service.data.Jobs[part.ID]
		if previous != 0 && job.ClientID <= previous {
			t.Fatal("counter wrap reversed batch dispatch order")
		}
		previous = job.ClientID
	}
}
func TestAmbiguousCheckpointCommitPreservesMediaAndCannotReplay(t *testing.T) {
	a, req := mixedFixture(t)
	s := a.Service
	writes := 0
	a.persist = func() error {
		writes++
		if writes == 1 {
			if err := s.saveLocked(); err != nil {
				return err
			}
		}
		return errors.New("synthetic error after rename; conservative retry cannot write")
	}
	d, err := a.Submit(context.Background(), req)
	if err != nil || d.State != "unknown" || s.mode != "offline" || len(s.data.Jobs) != 4 {
		t.Fatal("ambiguous commit was rolled back or remained sendable", d, err)
	}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: req.Account}
	for _, part := range d.Parts {
		if s.data.Jobs[part.ID].Media != nil {
			if _, err = cache.Load(part.ID); err != nil {
				t.Fatal("committed media was destroyed", err)
			}
		}
	}
	guards, err := filepath.Glob(filepath.Join(s.directory, "bot-intents", "*.json"))
	if err != nil || len(guards) != 1 {
		t.Fatal("missing recovery guard", guards, err)
	}
	beforeRecovery, readErr := s.checkpoint.Load()
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, job := range beforeRecovery.Jobs {
		if job.Status != "queued" {
			t.Fatal("fault injection did not leave ambiguous queued checkpoint")
		}
	}
	if err = RecoverBotIntents(s.directory); err != nil {
		t.Fatal(err)
	}
	data, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range data.Jobs {
		if job.Status != "unknown" {
			t.Fatal("restart could replay ambiguous queued part")
		}
	}
	guards, _ = filepath.Glob(filepath.Join(s.directory, "bot-intents", "*.json"))
	if len(guards) != 0 {
		t.Fatal("durably recovered guard not cleared")
	}
}
func TestRecoverIgnoresUnpublishedAtomicIntentTemp(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "bot-intents")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".write-interrupted"), []byte("unpublished"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RecoverBotIntents(root); err != nil {
		t.Fatal("uncommitted atomic temp blocked startup", err)
	}
}

func TestIntentRecoveryPreservesDefinitiveTerminalOutcomes(t *testing.T) {
	a, req := mixedFixture(t)
	s := a.Service
	a.persist = func() error {
		if err := s.saveLocked(); err != nil {
			return err
		}
		return errors.New("synthetic sync uncertainty")
	}
	d, err := a.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	data.Jobs[d.Parts[0].ID].Status = "accepted"
	data.Jobs[d.Parts[0].ID].ServerID = 789
	failed, remoteUnknown := "media_upload_rejected", "send_outcome_unknown"
	data.Jobs[d.Parts[1].ID].Status = "failed"
	data.Jobs[d.Parts[1].ID].ErrorCategory = &failed
	data.Jobs[d.Parts[2].ID].Status = "cancelled"
	data.Jobs[d.Parts[3].ID].Status = "unknown"
	data.Jobs[d.Parts[3].ID].ErrorCategory = &remoteUnknown
	if err = s.checkpoint.Save(data); err != nil {
		t.Fatal(err)
	}
	if err = RecoverBotIntents(s.directory); err != nil {
		t.Fatal(err)
	}
	recovered, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"accepted", "failed", "cancelled", "unknown"} {
		if recovered.Jobs[d.Parts[i].ID].Status != want {
			t.Fatal("terminal outcome overwritten", i, recovered.Jobs[d.Parts[i].ID].Status)
		}
	}
	if *recovered.Jobs[d.Parts[1].ID].ErrorCategory != failed || *recovered.Jobs[d.Parts[3].ID].ErrorCategory != remoteUnknown {
		t.Fatal("terminal failure category overwritten")
	}
}
func TestMixedBatchIsAtomicOrderedDurableAndReceiptAware(t *testing.T) {
	a, req := mixedFixture(t)
	s := a.Service
	d, err := a.Submit(context.Background(), req)
	if err != nil || len(d.Parts) != 4 || len(s.data.Jobs) != 4 {
		t.Fatal(d, err)
	}
	first := s.data.Jobs[d.Parts[0].ID]
	for i, p := range d.Parts {
		job := s.data.Jobs[p.ID]
		if job.ClientID != first.ClientID+uint32(i) || job.CreateTime != first.CreateTime {
			t.Fatal("native send ordering differs from input")
		}
	}
	loaded, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	s.data = loaded
	req.Epoch = s.modeEpoch
	again, err := a.Submit(context.Background(), req)
	if err != nil || again.ID != d.ID || len(s.data.Jobs) != 4 {
		t.Fatal("mixed batch replayed after reload", again, err)
	}
	for i, p := range d.Parts {
		job := s.data.Jobs[p.ID]
		job.Status = "accepted"
		job.ServerID = uint64(i + 100)
	}
	s.data.Jobs[d.Parts[3].ID].Status = "sending"
	pending, err := a.Delivery(req.Account, d.ID)
	if err != nil || pending.State == "accepted" {
		t.Fatal("in-flight part became a successful batch", pending, err)
	}
	s.data.Jobs[d.Parts[3].ID].Status = "accepted"
	accepted, _ := a.Delivery(req.Account, d.ID)
	if accepted.State != "accepted" || len(accepted.Parts) != 4 {
		t.Fatal(accepted)
	}
	s.data.Jobs[d.Parts[2].ID].Status = "unknown"
	unknown, _ := a.Submit(context.Background(), req)
	if unknown.State != "unknown" || len(s.data.Jobs) != 4 {
		t.Fatal("unknown batch was replayed or marked successful", unknown)
	}
	s.data.Jobs[d.Parts[2].ID].Status = "future-unrecognized"
	unexpected, _ := a.Delivery(req.Account, d.ID)
	if unexpected.State != "unknown" {
		t.Fatal("unrecognized state became accepted")
	}
}

func TestAcceptedStateWithoutReceiptCannotReportSuccess(t *testing.T) {
	a, req := mixedFixture(t)
	req.Outputs = req.Outputs[:1]
	d, err := a.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	a.Service.data.Jobs[d.ID].Status = "accepted"
	observed, err := a.Delivery(req.Account, d.ID)
	if err != nil || observed.State != "unknown" || observed.Error != "message_receipt_missing" {
		t.Fatal("unverified accepted state produced success", observed, err)
	}
}
func TestMixedBatchInvalidLaterPartDoesNotStageEarlierMedia(t *testing.T) {
	a, req := mixedFixture(t)
	req.Outputs[3].Text = strings.Repeat("x", 4097)
	if _, err := a.Submit(context.Background(), req); err == nil {
		t.Fatal("invalid batch accepted")
	}
	if len(a.Service.data.Jobs) != 0 || a.Service.data.NextClientID != 1 {
		t.Fatal("invalid batch published earlier outputs")
	}
	entries, err := os.ReadDir(filepath.Join(a.Service.directory, "media"))
	if err == nil && len(entries) > 0 {
		t.Fatal("invalid batch staged media before validation")
	}
}
func TestMixedBatchPersistenceFailureRollsBackAllPartsAndMedia(t *testing.T) {
	a, req := mixedFixture(t)
	s := a.Service
	blocked := filepath.Join(s.directory, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	s.checkpoint.Directory = blocked
	if _, err := a.Submit(context.Background(), req); err == nil {
		t.Fatal("checkpoint failure accepted")
	}
	if len(s.data.Jobs) != 0 || len(s.data.DeliveryBatches) != 0 || s.data.NextClientID != 1 {
		t.Fatal("failed commit left queue entries or counter changes")
	}
	entries, err := os.ReadDir(filepath.Join(s.directory, "media"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasSuffix(entry.Name(), ".key") {
			t.Fatal("failed commit left staged media", entry.Name())
		}
	}
}
