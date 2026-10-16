package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

func (h *Host) serveTopology(w http.ResponseWriter, r *http.Request, reply func(int, any)) {
	if r.URL.Path == "/topology" && r.Method == "GET" {
		reply(200, h.topology.view())
		return
	}
	if r.Method != "POST" {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTopologyBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if r.URL.Path == "/topology" {
		var input struct {
			Revision string       `json:"revision"`
			Plan     topologyPlan `json:"plan"`
		}
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_topology_config"})
			return
		}
		if err := h.topology.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "topology_save_failed", "category": topologyErrorCategory(err)})
			return
		}
		reply(200, h.topology.view())
		return
	}
	var body struct{}
	if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF {
		reply(400, map[string]string{"error": "expected_empty_json_object"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.topology.apply(ctx); err != nil {
		reply(409, map[string]any{"error": "topology_apply_failed", "category": topologyErrorCategory(err), "status": h.topology.view()})
		return
	}
	reply(200, h.topology.view())
}

func topologyErrorCategory(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "operation_deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "operation_cancelled"
	}
	switch err.Error() {
	case "listener_batch_revision_conflict", "listener_batch_unavailable", "invalid_listener_batch",
		"invalid_listener_configuration", "listener_bind_failed", "listener_not_listening",
		"topology_revision_conflict", "topology_config_invalid", "active_account_bootstrap_immutable",
		"stop_instance_before_rename", "runtime_stopping", "runtime_not_running", "topology_config_too_large",
		"invalid_runtime_topology", "invalid_account_configuration", "duplicate_listener",
		"token_env_unavailable_or_invalid", "token_env_required", "connection_limit",
		"invalid_http_post_configuration", "invalid_reverse_configuration", "native_transport_unavailable":
		return err.Error()
	default:
		return "topology_operation_failed"
	}
}
