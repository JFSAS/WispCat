package host

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (h *Host) serveManagement(w http.ResponseWriter, r *http.Request, reply func(int, any)) {
	if r.Method == "GET" && r.URL.Path == "/management" {
		reply(200, h.management.view())
		return
	}
	if strings.HasPrefix(r.URL.Path, "/management/operations/") {
		if r.Method != "GET" {
			reply(405, map[string]string{"error": "method_not_allowed"})
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/management/operations/")
		if len(id) != 32 {
			reply(404, map[string]string{"error": "management_operation_not_found"})
			return
		}
		if value, ok := h.management.operationView(id); ok {
			reply(200, value)
		} else {
			reply(404, map[string]string{"error": "management_operation_not_found"})
		}
		return
	}
	if r.Method != "POST" {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if r.URL.Path == "/management" {
		var input struct {
			Revision string         `json:"revision"`
			Plan     managementPlan `json:"plan"`
		}
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_management_config"})
			return
		}
		if err := h.management.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "management_save_failed", "category": managementErrorCategory(err)})
			return
		}
		reply(200, h.management.view())
		return
	}
	var empty map[string]json.RawMessage
	if decoder.Decode(&empty) != nil || empty == nil || len(empty) != 0 || decoder.Decode(new(any)) != io.EOF {
		reply(400, map[string]string{"error": "expected_empty_json_object"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	op, err := h.management.begin(ctx)
	if err != nil {
		reply(409, map[string]string{"error": "management_apply_failed", "category": managementErrorCategory(err)})
		return
	}
	view, _ := h.management.operationView(op.ID)
	raw, _ := json.Marshal(map[string]any{"operation": view, "management_address": op.Address, "management_token_env": op.plan.TokenEnv})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(http.StatusAccepted)
	if _, err = w.Write(raw); err == nil {
		err = http.NewResponseController(w).Flush()
	}
	if err != nil {
		h.management.abort(op, "management_response_write_failed")
		return
	}
	// With an explicit length and successful flush the caller has the complete
	// acknowledgement/new address before its old connection can be retired.
	h.management.dispatch(op)
}

func managementErrorCategory(err error) string {
	switch err.Error() {
	case "management_operation_pending", "management_revision_conflict", "management_config_invalid",
		"management_config_too_large", "listener_bind_failed", "listener_closed", "listener_change_pending",
		"runtime_stopping", "runtime_not_running", "runtime_not_started":
		return err.Error()
	default:
		return topologyErrorCategory(err)
	}
}
