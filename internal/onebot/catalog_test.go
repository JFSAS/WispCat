package onebot

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/JFSAS/wispcat/internal/catalog"
)

func TestGeneratedMCPCatalogMatchesExecutableActions(t *testing.T) {
	raw, err := json.Marshal((&Runtime{}).Actions())
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err = json.Compact(&compact, catalog.JSON); err != nil || !bytes.Equal(raw, compact.Bytes()) {
		t.Fatal("MCP catalog drift; regenerate with wispcat catalog")
	}
}
