//go:build storagefixtures

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

// This exercises the production normalized submission/checkpoint/archive path.
// Platform acknowledgements are synthetic; no protocol worker is started.
func TestArchiveCumulativeCapacity(t *testing.T) {
	a, template := archiveFixture(t)
	s := a.Service
	s.data.NextClientID = 1
	const total = 10050
	request := bot.Send{Account: template.Account, Epoch: template.Epoch, Conversation: template.Conversation, Text: "synthetic capacity reply"}
	firstID, firstMessage := "", ""
	peak := 0
	started := time.Now()
	for index := 0; index < total; index++ {
		request.Key = fmt.Sprintf("capacity-%d", index)
		delivery, err := a.Submit(context.Background(), request)
		if err != nil {
			t.Fatal("cumulative submissions reached the old journal ceiling", index, err)
		}
		job := s.data.Jobs[delivery.ID]
		if job == nil {
			t.Fatal("new request was not queued", index)
		}
		job.Status, job.ServerID = "accepted", uint64(100000+index)
		if index == 0 {
			firstID, firstMessage = delivery.ID, a.receiptMessageID(job)
		}
		peak = max(peak, len(s.data.Jobs))
		if (index+1)%1000 == 0 {
			t.Logf("synthetic submissions committed: %d; active jobs: %d", index+1, len(s.data.Jobs))
		}
	}
	for len(s.data.Jobs) > 0 {
		report, err := a.ArchiveCompleted(context.Background())
		if err != nil || report.ArchivedJobs == 0 {
			t.Fatal("terminal archive did not drain", report, err)
		}
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ArchivedJobs != total || stats.ArchivedBatches != total || peak > archiveHighWater+20 {
		t.Fatal("archive counters or active bound mismatch", stats, peak, err)
	}
	request.Key = "capacity-0"
	again, err := a.Submit(context.Background(), request)
	if err != nil || again.ID != firstID || again.MessageID != firstMessage || again.State != "accepted" || len(s.data.Jobs) != 0 {
		t.Fatal("oldest request was requeued or lost its receipt", err)
	}
	if err = a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDeliveryArchive(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.Recover(s.directory); err != nil {
		t.Fatal(err)
	}
	a.Archive = reopened
	data, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	s.data = data
	again, err = a.Submit(context.Background(), request)
	if err != nil || again.ID != firstID || again.MessageID != firstMessage {
		t.Fatal("restart lost the oldest archived request", err)
	}
	report, err := json.Marshal(map[string]any{"scope": "synthetic normalized submissions, active checkpoint and terminal archive; not live delivery or a long-duration soak", "system": runtime.GOOS, "architecture": runtime.GOARCH, "submissions": total, "archived_jobs": stats.ArchivedJobs, "peak_active_jobs": peak, "archive_file_bytes": stats.FileBytes, "elapsed_millis": time.Since(started).Milliseconds(), "oldest_request_stable_after_restart": true, "production_protocol_worker": false, "live_wechat_requests": 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("DELIVERY_ARCHIVE_LOAD_REPORT=" + string(report))
}
