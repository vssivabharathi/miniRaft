package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClusterAPI(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("GET", "/cluster", nil)
	rr := httptest.NewRecorder()

	d.handleGetCluster(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	var summary ClusterSummary
	if err := json.NewDecoder(rr.Body).Decode(&summary); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if summary.Leader != leaderID {
		t.Errorf("expected leader %d, got %d", leaderID, summary.Leader)
	}
	if summary.Term != term {
		t.Errorf("expected term %d, got %d", term, summary.Term)
	}
	if len(summary.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(summary.Nodes))
	}
}

func TestMetricsAPI(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()

	d.handleGetMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	var metrics []MetricsSnapshot
	if err := json.NewDecoder(rr.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(metrics) != 3 {
		t.Errorf("expected 3 metrics snapshots, got %d", len(metrics))
	}
}

func TestKillNodeAPI(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("POST", "/node/2/kill", nil)
	req.SetPathValue("id", "2")
	rr := httptest.NewRecorder()

	d.handleKillNode(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	if c.IsAlive(2) {
		t.Errorf("expected node 2 to be killed")
	}
}

func TestRestartNodeAPI(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)
	c.Kill(2)
	// Give the node's goroutines a moment to exit to avoid a false-positive
	// data race between old runElectionTicker and the restarted node.
	time.Sleep(100 * time.Millisecond)

	d := &Dashboard{cluster: c}
	req := httptest.NewRequest("POST", "/node/2/restart", nil)
	req.SetPathValue("id", "2")
	rr := httptest.NewRecorder()

	d.handleRestartNode(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	if !c.IsAlive(2) {
		t.Errorf("expected node 2 to be restarted")
	}
}

func TestSubmitCommandAPI(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	defer c.Shutdown()

	mustWaitForLeader(t, c, 5*time.Second)

	d := &Dashboard{cluster: c}
	
	body := []byte(`{"command": "SET X 1"}`)
	req := httptest.NewRequest("POST", "/command", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	d.handleSubmitCommand(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %v", rr.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := resp["index"]; !ok {
		t.Errorf("expected index in response")
	}
	if _, ok := resp["term"]; !ok {
		t.Errorf("expected term in response")
	}
}
