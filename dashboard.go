package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type Dashboard struct {
	cluster *Cluster
}

type NodeSummary struct {
	ID          int    `json:"id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	CommitIndex int    `json:"commitIndex"`
	LogLength   int    `json:"logLength"`
}

type ClusterSummary struct {
	Leader int           `json:"leader"`
	Term   int           `json:"term"`
	Nodes  []NodeSummary `json:"nodes"`
}

// StartDashboard starts a background HTTP server serving the dashboard UI and API.
func StartDashboard(c *Cluster, addr string) *http.Server {
	d := &Dashboard{cluster: c}
	mux := http.NewServeMux()

	// Serve static files (HTML, CSS, JS)
	mux.Handle("/", http.FileServer(http.Dir("static")))

	// API endpoints
	mux.HandleFunc("GET /cluster", d.handleGetCluster)
	mux.HandleFunc("GET /metrics", d.handleGetMetrics)
	mux.HandleFunc("GET /metrics/prometheus", d.handleGetPrometheus)
	mux.HandleFunc("POST /node/{id}/kill", d.handleKillNode)
	mux.HandleFunc("POST /node/{id}/restart", d.handleRestartNode)
	mux.HandleFunc("POST /command", d.handleSubmitCommand)

	// Observability Endpoints
	mux.HandleFunc("GET /api/logs", d.handleGetLogs)
	mux.HandleFunc("GET /api/snapshots", d.handleGetSnapshots)
	mux.HandleFunc("GET /api/state-machine", d.handleGetStateMachine)
	mux.HandleFunc("GET /api/node/{id}", d.handleGetNodeInfo)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Dashboard server error: %v\n", err)
		}
	}()
	return srv
}

func (d *Dashboard) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	leaderID, term := d.cluster.GetLeader()

	var nodes []NodeSummary
	for id := 1; id <= d.cluster.NodeCount(); id++ {
		if !d.cluster.IsAlive(id) {
			nodes = append(nodes, NodeSummary{
				ID:    id,
				State: "DEAD",
			})
			continue
		}

		node := d.cluster.GetNode(id)
		if node != nil {
			snap := node.GetMetricsSnapshot()
			nodes = append(nodes, NodeSummary{
				ID:          id,
				State:       snap.State.String(),
				Term:        snap.CurrentTerm,
				CommitIndex: snap.CommitIndex,
				LogLength:   snap.LogLength,
			})
		}
	}

	summary := ClusterSummary{
		Leader: leaderID,
		Term:   term,
		Nodes:  nodes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

func (d *Dashboard) handleGetMetrics(w http.ResponseWriter, r *http.Request) {
	var metrics []MetricsSnapshot
	for id := 1; id <= d.cluster.NodeCount(); id++ {
		if d.cluster.IsAlive(id) {
			node := d.cluster.GetNode(id)
			if node != nil {
				metrics = append(metrics, node.GetMetricsSnapshot())
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (d *Dashboard) handleKillNode(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	d.cluster.Kill(id)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"killed"}`))
}

func (d *Dashboard) handleRestartNode(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	err = d.cluster.Restart(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"restarted"}`))
}

func (d *Dashboard) handleSubmitCommand(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	index, term, err := d.cluster.SubmitCommand(req.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"index": index,
		"term":  term,
	})
}

// ---------------------------------------------------------
// Observability API Handlers
// ---------------------------------------------------------

func (d *Dashboard) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	nodes := make([]NodeLogSummary, 0)
	for id := 1; id <= d.cluster.NodeCount(); id++ {
		if d.cluster.IsAlive(id) {
			if node := d.cluster.GetNode(id); node != nil {
				nodes = append(nodes, node.GetLogSummary())
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"nodes": nodes})
}

func (d *Dashboard) handleGetSnapshots(w http.ResponseWriter, r *http.Request) {
	nodes := make([]NodeSnapshotSummary, 0)
	for id := 1; id <= d.cluster.NodeCount(); id++ {
		if d.cluster.IsAlive(id) {
			if node := d.cluster.GetNode(id); node != nil {
				nodes = append(nodes, node.GetSnapshotSummary())
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"nodes": nodes})
}

func (d *Dashboard) handleGetStateMachine(w http.ResponseWriter, r *http.Request) {
	nodes := make([]NodeStateMachineSummary, 0)
	for id := 1; id <= d.cluster.NodeCount(); id++ {
		if d.cluster.IsAlive(id) {
			if node := d.cluster.GetNode(id); node != nil {
				nodes = append(nodes, node.GetStateMachineSummary())
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"nodes": nodes})
}

func (d *Dashboard) handleGetNodeInfo(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	if !d.cluster.IsAlive(id) {
		http.Error(w, "node is dead", http.StatusServiceUnavailable)
		return
	}

	node := d.cluster.GetNode(id)
	if node == nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node.GetFullSummary())
}
