package catalog

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDiscoveryIndexRetainsExecutableMetadataWithoutSchemas(t *testing.T) {
	all := Actions()
	index := Search("", "")
	if index.Count != len(all) || len(index.Actions) != len(all) {
		t.Fatal(index)
	}
	for i, entry := range index.Actions {
		full := all[i]
		if entry.Name != full.Name || entry.Category == "" || entry.Category != full.Category || entry.ReadOnly != full.ReadOnly || entry.Streaming != full.Streaming {
			t.Fatal("discovery projection drift", full, entry)
		}
	}
	raw, err := json.Marshal(index)
	if err != nil || bytes.Contains(raw, []byte("input_schema")) {
		t.Fatal("compact search included full schemas", err)
	}
	for _, category := range Categories() {
		if category.Name == "" || category.Count != len(List(category.Name)) {
			t.Fatal("category count drift", category)
		}
	}
	count := 0
	previous := ""
	for _, category := range Categories() {
		if category.Name <= previous {
			t.Fatal("categories have unstable/duplicate order")
		}
		previous = category.Name
		count += category.Count
	}
	if count != len(all) {
		t.Fatal("category partition did not cover actions", count, len(all))
	}
	// Returned schemas belong to this query, not later concurrent queries.
	all[0].Schema[0] = '!'
	fresh, ok := Find(all[0].Name)
	if !ok || !json.Valid(fresh.Schema) {
		t.Fatal("caller schema mutation corrupted future queries")
	}
}

func TestDiscoverySearchAndCategoryFilters(t *testing.T) {
	result := Search("groups", "  MEMBER  ")
	if result.Count != 1 || result.Actions[0].Name != "get_group_member_info" || !result.Actions[0].ReadOnly {
		t.Fatal(result)
	}
	if result := Search("messages", "member"); result.Count != 0 || result.Actions == nil {
		t.Fatal("category/query intersection ignored", result)
	}
	if result := Search("", "platform requests"); result.Count != 1 || result.Actions[0].Name != "test_download_stream" || !result.Actions[0].Streaming {
		t.Fatal("summary search unavailable", result)
	}
	if result := Search("", "STREAMS"); result.Count != len(List("streams")) || result.Count == 0 {
		t.Fatal("category text was not searchable", result)
	}
	if len(List("unknown-category")) != 0 || Search("unknown-category", "").Count != 0 {
		t.Fatal("unknown category fabricated actions")
	}
	if result := Search("", "delete_msg"); result.Count != 0 {
		t.Fatal("unsupported native capability advertised", result)
	}
}
