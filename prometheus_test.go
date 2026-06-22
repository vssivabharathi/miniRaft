package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrometheusEndpoint(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("GET", "/metrics/prometheus", nil)
	rr := httptest.NewRecorder()

	d.handleGetPrometheus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "text/plain; version=0.0.4" {
		t.Errorf("unexpected content type: %v", contentType)
	}

	body, _ := io.ReadAll(rr.Body)
	out := string(body)

	if !strings.Contains(out, "mini_raft_current_term{node=\"1\"}") {
		t.Errorf("missing metric for node 1 current term")
	}
	if !strings.Contains(out, "mini_raft_state{node=") {
		t.Errorf("missing metric for state")
	}
}

func TestPrometheusMetricsFormat(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("GET", "/metrics/prometheus", nil)
	rr := httptest.NewRecorder()

	d.handleGetPrometheus(rr, req)

	body, _ := io.ReadAll(rr.Body)
	out := string(body)

	expectedMetrics := []string{
		"mini_raft_current_term",
		"mini_raft_state",
		"mini_raft_elections_won",
		"mini_raft_elections_lost",
		"mini_raft_rpc_sent",
		"mini_raft_rpc_received",
		"mini_raft_heartbeats_sent",
		"mini_raft_heartbeats_received",
		"mini_raft_commands_committed",
		"mini_raft_commands_applied",
		"mini_raft_commit_index",
		"mini_raft_log_length",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(out, metric) {
			t.Errorf("missing metric %s in output", metric)
		}
	}
}

func TestPrometheusNodeLabels(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("GET", "/metrics/prometheus", nil)
	rr := httptest.NewRecorder()

	d.handleGetPrometheus(rr, req)

	body, _ := io.ReadAll(rr.Body)
	out := string(body)

	// Ensure we export for all 3 nodes
	if !strings.Contains(out, `node="1"`) {
		t.Errorf("missing node 1 label")
	}
	if !strings.Contains(out, `node="2"`) {
		t.Errorf("missing node 2 label")
	}
	if !strings.Contains(out, `node="3"`) {
		t.Errorf("missing node 3 label")
	}
}
