package catalog

import (
	"sort"
	"strings"
)

// Entry is a compact discovery record. Get the full Action for input schemas.
type Entry struct {
	Name      string `json:"name"`
	Category  string `json:"category"`
	Summary   string `json:"summary"`
	ReadOnly  bool   `json:"read_only"`
	Streaming bool   `json:"streaming"`
}

type SearchResult struct {
	Count   int     `json:"count"`
	Actions []Entry `json:"actions"`
}

type Category struct {
	Name  string `json:"category"`
	Count int    `json:"count"`
}

// List keeps the full-record array contract, optionally restricted by category.
func List(category string) []Action {
	filtered := []Action{}
	for _, action := range Actions() {
		if category == "" || action.Category == category {
			filtered = append(filtered, action)
		}
	}
	return filtered
}

// Search matches canonical names, summaries and categories case-insensitively.
// It introduces no aliases or actions beyond the executable source projection.
func Search(category, query string) SearchResult {
	query = strings.ToLower(strings.TrimSpace(query))
	result := SearchResult{Actions: []Entry{}}
	for _, action := range List(category) {
		if strings.Contains(strings.ToLower(action.Name), query) || strings.Contains(strings.ToLower(action.Summary), query) || strings.Contains(strings.ToLower(action.Category), query) {
			result.Actions = append(result.Actions, Entry{Name: action.Name, Category: action.Category, Summary: action.Summary, ReadOnly: action.ReadOnly, Streaming: action.Streaming})
		}
	}
	result.Count = len(result.Actions)
	return result
}

// Categories derives stable category counts from the same executable actions.
func Categories() []Category {
	counts := map[string]int{}
	for _, action := range Actions() {
		counts[action.Category]++
	}
	items := []Category{}
	for name, count := range counts {
		items = append(items, Category{Name: name, Count: count})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items
}
