package mcp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JFSAS/wispcat/internal/catalog"
	"github.com/JFSAS/wispcat/pkg/client"
)

func catalogRPC(id int, name, arguments string) string {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": name, "arguments": json.RawMessage(arguments)}})
	return string(raw)
}

func catalogPayload(t *testing.T, reply mcpReply, failed bool, target any) {
	t.Helper()
	var result struct {
		Failed bool `json:"isError"`
		Items  []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if reply.Error != nil || json.Unmarshal(reply.Result, &result) != nil || result.Failed != failed || len(result.Items) != 1 || result.Items[0].Type != "text" {
		t.Fatal(reply, result)
	}
	if json.Unmarshal([]byte(result.Items[0].Text), target) != nil {
		t.Fatal("catalog result was not the declared JSON shape", result)
	}
}

func TestMCPCatalogDiscoveryAcrossModesWithoutExecution(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer peer.Close()
	c, _ := client.New(peer.URL, "synthetic-token")
	for _, mode := range []string{"docs", "read", "write"} {
		t.Run(mode, func(t *testing.T) {
			s := startMCPSession(t, &Server{Mode: mode, Client: c})
			s.send(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
			var tools struct {
				Tools []struct {
					Name        string          `json:"name"`
					Annotations map[string]bool `json:"annotations"`
					Schema      map[string]any  `json:"inputSchema"`
				} `json:"tools"`
			}
			if json.Unmarshal(s.next().Result, &tools) != nil {
				t.Fatal("invalid tool schemas")
			}
			seen := map[string]bool{}
			for _, tool := range tools.Tools {
				seen[tool.Name] = true
				if tool.Name == "query_action" || tool.Name == "invoke_action" {
					continue
				}
				if !tool.Annotations["readOnlyHint"] || tool.Annotations["openWorldHint"] || tool.Annotations["destructiveHint"] || tool.Schema["additionalProperties"] != false {
					t.Fatal("discovery tool advertised side effects or an unconstrained schema", tool)
				}
			}
			for _, name := range []string{"list_actions", "get_action", "search_actions", "list_categories"} {
				if !seen[name] {
					t.Fatal("discovery tool absent", name)
				}
			}
			if seen["query_action"] != (mode != "docs") || seen["invoke_action"] != (mode == "write") {
				t.Fatal("execution visibility changed", seen)
			}
			s.send(catalogRPC(2, "list_actions", `{}`))
			var all []catalog.Action
			catalogPayload(t, s.next(), false, &all)
			if len(all) == 0 || len(all) != len(catalog.Actions()) || len(all[0].Schema) == 0 {
				t.Fatal("legacy full-array contract changed")
			}
			s.send(catalogRPC(3, "list_categories", `{}`))
			var categories []catalog.Category
			catalogPayload(t, s.next(), false, &categories)
			total := 0
			for _, item := range categories {
				total += item.Count
			}
			if total != len(all) {
				t.Fatal("category counts disagree with full catalog")
			}
			s.send(catalogRPC(4, "search_actions", `{"query":"  GROUP_MEMBER  ","category":"groups"}`))
			var found catalog.SearchResult
			catalogPayload(t, s.next(), false, &found)
			if found.Count != 1 || found.Actions[0].Name != "get_group_member_info" {
				t.Fatal(found)
			}
			s.send(catalogRPC(5, "get_action", `{"name":"get_group_member_info"}`))
			var action catalog.Action
			catalogPayload(t, s.next(), false, &action)
			if action.Category != "groups" || action.Name != found.Actions[0].Name || !strings.Contains(string(action.Schema), "group_id") {
				t.Fatal("full documentation did not resolve search result", action)
			}
			s.send(catalogRPC(6, "list_actions", `{"category":"streams"}`))
			var streams []catalog.Action
			catalogPayload(t, s.next(), false, &streams)
			if len(streams) == 0 {
				t.Fatal("category filtering returned no stream actions")
			}
			for _, action := range streams {
				if action.Category != "streams" {
					t.Fatal("filter included another category", action)
				}
			}
			s.finish()
		})
	}
	if calls.Load() != 0 {
		t.Fatal("catalog discovery contacted an execution endpoint")
	}
}

func TestMCPCatalogArgumentsMatchPublishedSchemas(t *testing.T) {
	s := startMCPSession(t, &Server{Mode: "docs"})
	bad := []struct{ tool, arguments string }{
		{"list_actions", `{"CATEGORY":"streams"}`}, {"get_action", `{"NAME":"get_status"}`},
		{"search_actions", `{"QUERY":"stream"}`}, {"search_actions", `{"Query":"stream"}`},
		{"search_actions", `{"query":"absent","QUERY":"stream"}`},
		{"search_actions", `{"query":"stream","Category":"streams"}`},
		{"list_actions", `{"category":null}`}, {"list_actions", `{"category":1}`}, {"list_actions", `{"query":"unused"}`},
		{"search_actions", `{}`}, {"search_actions", `{"query":null}`}, {"search_actions", `{"query":1}`}, {"search_actions", `{"query":[]}`},
		{"search_actions", `{"query":"send","unexpected":true}`}, {"search_actions", `{"query":"","category":null}`},
		{"get_action", `{}`}, {"get_action", `{"name":""}`}, {"get_action", `{"name":null}`}, {"get_action", `{"name":12}`},
		{"get_action", `{"name":"get_status","category":"system"}`}, {"list_categories", `{"category":"streams"}`},
		{"list_categories", `null`}, {"list_categories", `[]`},
		{"search_actions", `{"query":"` + strings.Repeat("查", 257) + `"}`},
		{"list_actions", `{"category":"` + strings.Repeat("a", 65) + `"}`},
	}
	for i, tc := range bad {
		s.send(catalogRPC(i, tc.tool, tc.arguments))
		var message string
		catalogPayload(t, s.next(), true, &message)
		if message != "invalid_catalog_arguments" {
			t.Fatal(tc, message)
		}
	}
	// JSON Schema maxLength counts characters, not UTF-8 bytes.
	s.send(catalogRPC(100, "search_actions", `{"query":"`+strings.Repeat("查", 256)+`"}`))
	var result catalog.SearchResult
	catalogPayload(t, s.next(), false, &result)
	if result.Count != 0 || result.Actions == nil {
		t.Fatal(result)
	}
	s.send(catalogRPC(101, "get_action", `{"name":"delete_msg"}`))
	var message string
	catalogPayload(t, s.next(), true, &message)
	if message != "action_unavailable" {
		t.Fatal("unsupported capability acquired documentation", message)
	}
	s.finish()
}
