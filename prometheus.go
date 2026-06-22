package main

import (
	"fmt"
	"net/http"
)

// handleGetPrometheus exposes cluster metrics in the standard Prometheus text-based format.
// It directly iterates over all nodes and queries their metrics lock-free.
func (d *Dashboard) handleGetPrometheus(w http.ResponseWriter, r *http.Request) {
	// Prometheus expects this specific Content-Type
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	for id := 1; id <= d.cluster.NodeCount(); id++ {
		// Only export metrics for nodes that are alive
		if !d.cluster.IsAlive(id) {
			continue
		}

		node := d.cluster.GetNode(id)
		if node == nil {
			continue
		}

		// Thread-safe metrics snapshot retrieval
		snap := node.GetMetricsSnapshot()

		// Map string state to numeric for Prometheus (0=Follower, 1=Leader, 2=Candidate)
		stateVal := 0
		switch snap.State {
		case Leader:
			stateVal = 1
		case Candidate:
			stateVal = 2
		}

		// Node labels
		labels := fmt.Sprintf(`node="%d"`, id)
		stateLabels := fmt.Sprintf(`node="%d",state="%s"`, id, snap.State.String())

		// Write Prometheus metrics format
		fmt.Fprintf(w, "mini_raft_current_term{%s} %d\n", labels, snap.CurrentTerm)
		fmt.Fprintf(w, "mini_raft_state{%s} %d\n", stateLabels, stateVal)
		fmt.Fprintf(w, "mini_raft_elections_won{%s} %d\n", labels, snap.ElectionsWon)
		fmt.Fprintf(w, "mini_raft_elections_lost{%s} %d\n", labels, snap.ElectionsLost)
		fmt.Fprintf(w, "mini_raft_rpc_sent{%s} %d\n", labels, snap.RPCSent)
		fmt.Fprintf(w, "mini_raft_rpc_received{%s} %d\n", labels, snap.RPCReceived)
		fmt.Fprintf(w, "mini_raft_heartbeats_sent{%s} %d\n", labels, snap.HeartbeatsSent)
		fmt.Fprintf(w, "mini_raft_heartbeats_received{%s} %d\n", labels, snap.HeartbeatsReceived)
		fmt.Fprintf(w, "mini_raft_commands_committed{%s} %d\n", labels, snap.CommandsCommitted)
		fmt.Fprintf(w, "mini_raft_commands_applied{%s} %d\n", labels, snap.CommandsApplied)
		fmt.Fprintf(w, "mini_raft_commit_index{%s} %d\n", labels, snap.CommitIndex)
		fmt.Fprintf(w, "mini_raft_log_length{%s} %d\n", labels, snap.LogLength)
	}
}
