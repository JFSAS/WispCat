// Package catalog contains a build-time projection of the executable action
// catalog. MCP docs mode can use it without loading the WeChat implementation.
package catalog

import (
	_ "embed"
	"encoding/json"
)

//go:embed actions.json
var JSON []byte

type Action struct {
	Name      string          `json:"name"`
	Summary   string          `json:"summary"`
	ReadOnly  bool            `json:"read_only"`
	Schema    json.RawMessage `json:"input_schema"`
	Streaming bool            `json:"streaming,omitempty"`
}

func Actions() []Action {
	var actions []Action
	if err := json.Unmarshal(JSON, &actions); err != nil {
		panic("invalid embedded action catalog")
	}
	return actions
}
func Find(name string) (Action, bool) {
	for _, a := range Actions() {
		if a.Name == name {
			return a, true
		}
	}
	return Action{}, false
}
