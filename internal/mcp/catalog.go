package mcp

import (
	"bytes"
	"encoding/json"
	"slices"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/catalog"
)

func catalogTools() []any {
	return []any{
		tool("list_actions", "List full action records and parameter schemas, optionally by category; search_actions returns a compact index", map[string]any{"category": map[string]any{"type": "string", "maxLength": 64}}, true),
		tool("get_action", "Get one canonical action and its complete parameter schema", map[string]any{"name": map[string]any{"type": "string", "minLength": 1, "maxLength": 128}}, true),
		tool("search_actions", "Search canonical action names, summaries and categories; returns a compact count/actions index without parameter schemas", map[string]any{"query": map[string]any{"type": "string", "maxLength": 256}, "category": map[string]any{"type": "string", "maxLength": 64}}, true),
		tool("list_categories", "List categories and their action counts, derived from executable actions", map[string]any{}, true),
	}
}

func decodeCatalogArguments(raw json.RawMessage, target any, allowed ...string) bool {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	// Every current catalog argument is a string. A present JSON null is not
	// an omitted optional argument, even though decoding a *string accepts it.
	for name, value := range fields {
		// Go struct decoding accepts case-insensitive tag matches, but JSON
		// Schema property names are exact, including mixed-case duplicates.
		if !slices.Contains(allowed, name) || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return false
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target) == nil
}

func catalogCall(name string, raw json.RawMessage) any {
	switch name {
	case "list_actions":
		var args struct {
			Category *string `json:"category"`
		}
		if !decodeCatalogArguments(raw, &args, "category") || !catalogText(args.Category, 64, false) {
			return content("invalid_catalog_arguments", true)
		}
		return content(catalog.List(optionalText(args.Category)), false)
	case "search_actions":
		var args struct {
			Query    *string `json:"query"`
			Category *string `json:"category"`
		}
		if !decodeCatalogArguments(raw, &args, "query", "category") || !catalogText(args.Query, 256, true) || !catalogText(args.Category, 64, false) {
			return content("invalid_catalog_arguments", true)
		}
		return content(catalog.Search(optionalText(args.Category), *args.Query), false)
	case "get_action":
		var args struct {
			Name *string `json:"name"`
		}
		if !decodeCatalogArguments(raw, &args, "name") || !catalogText(args.Name, 128, true) || *args.Name == "" {
			return content("invalid_catalog_arguments", true)
		}
		action, ok := catalog.Find(*args.Name)
		if !ok {
			return content("action_unavailable", true)
		}
		return content(action, false)
	case "list_categories":
		if !decodeCatalogArguments(raw, &struct{}{}) {
			return content("invalid_catalog_arguments", true)
		}
		return content(catalog.Categories(), false)
	}
	return content("tool_unavailable", true)
}

func optionalText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func catalogText(value *string, maximum int, required bool) bool {
	return value == nil && !required || value != nil && utf8.RuneCountInString(*value) <= maximum
}
