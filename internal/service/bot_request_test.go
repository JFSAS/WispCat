package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/JFSAS/wispcat/pkg/bot"
)

func TestRequestLookupPreservesActiveArchivedAndReopenedOutcome(t *testing.T) {
	a, request := archiveFixture(t)
	delivery := acknowledgeArchiveFixture(t, a, request)
	a.Service.data.Jobs[delivery.Parts[1].ID].Status = "unknown"
	before, err := a.Delivery(request.Account, delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		snapshot, _ := json.Marshal(a.Service.data)
		counter := a.Service.data.NextClientID
		got, err := a.DeliveryByRequest(request.Account, request.Key)
		if err != nil || !reflect.DeepEqual(got, before) {
			t.Fatal("request lookup changed or lost the outcome", err, got)
		}
		after, _ := json.Marshal(a.Service.data)
		if string(snapshot) != string(after) || counter != a.Service.data.NextClientID {
			t.Fatal("lookup mutated queue/checkpoint/counters")
		}
	}
	check()
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(a.Service.data.Jobs) != 0 || len(a.Service.data.DeliveryBatches) != 0 {
		t.Fatal("fixture did not move the request out of the active checkpoint")
	}
	check()
	_ = a.Archive.Close()
	archive, err := OpenDeliveryArchive(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	a.Archive = archive
	t.Cleanup(func() { _ = archive.Close() })
	check()
}

func TestRequestLookupSeparatesMissingScopeAndStorageFailure(t *testing.T) {
	a, request := archiveFixture(t)
	if _, err := a.DeliveryByRequest(request.Account, "missing"); !errors.Is(err, bot.ErrNotFound) {
		t.Fatal("missing request became an outcome", err)
	}
	if _, err := a.DeliveryByRequest("other-account", request.Key); !errors.Is(err, bot.ErrScope) {
		t.Fatal("lookup crossed account scope", err)
	}
	acknowledgeArchiveFixture(t, a, request)
	if _, err := a.DeliveryByRequest(request.Account, "bot/"+request.Key); !errors.Is(err, bot.ErrNotFound) {
		t.Fatal("internal key prefix became a caller alias", err)
	}
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = a.Archive.Close()
	if _, err := a.DeliveryByRequest(request.Account, request.Key); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("storage failure became not-found or success", err)
	}
}

func TestRequestLookupRetainsUnconfirmedStatesAndRejectsMissingJobs(t *testing.T) {
	a, request := mixedFixture(t)
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"queued", "sending", "uploading", "submitting", "unknown", "failed", "cancelled", "accepted"} {
		for _, part := range delivery.Parts {
			a.Service.data.Jobs[part.ID].Status = state
			a.Service.data.Jobs[part.ID].ServerID = 0
		}
		got, err := a.DeliveryByRequest(request.Account, request.Key)
		want := state
		if state == "accepted" {
			want = "unknown"
		}
		if err != nil || got.State != want || got.MessageID != "" {
			t.Fatal("lookup fabricated a confirmed receipt", state, got, err)
		}
	}
	delete(a.Service.data.Jobs, delivery.ID)
	if _, err := a.DeliveryByRequest(request.Account, request.Key); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("broken request index appeared unused", err)
	}
}
