package main

import (
	"fmt"
	"sync/atomic"
)

// Metrics holds lock-free counters for operational visibility.
type Metrics struct {
	ElectionsWon       atomic.Int64
	ElectionsLost      atomic.Int64
	RPCSent            atomic.Int64
	RPCReceived        atomic.Int64
	CommandsCommitted  atomic.Int64
	CommandsApplied    atomic.Int64
	HeartbeatsSent     atomic.Int64
	HeartbeatsReceived atomic.Int64
}

// MetricsSnapshot represents a point-in-time view of a node's operational metrics.
type MetricsSnapshot struct {
	NodeID             int
	State              NodeState
	CurrentTerm        int
	ElectionsWon       int64
	ElectionsLost      int64
	RPCSent            int64
	RPCReceived        int64
	CommandsCommitted  int64
	CommandsApplied    int64
	HeartbeatsSent     int64
	HeartbeatsReceived int64
	CommitIndex        int
	LastApplied        int
	LogLength          int
}

// GetMetricsSnapshot safely captures all atomic metrics and current volatile state.
func (n *Node) GetMetricsSnapshot() MetricsSnapshot {
	// Acquire a read lock to snapshot the volatile/persistent state safely
	n.mu.RLock()
	state := n.state
	term := n.currentTerm
	commitIndex := n.commitIndex
	lastApplied := n.lastApplied
	logLength := len(n.log)
	n.mu.RUnlock()

	// Atomic loads do not require a mutex lock
	return MetricsSnapshot{
		NodeID:             n.id,
		State:              state,
		CurrentTerm:        term,
		ElectionsWon:       n.metrics.ElectionsWon.Load(),
		ElectionsLost:      n.metrics.ElectionsLost.Load(),
		RPCSent:            n.metrics.RPCSent.Load(),
		RPCReceived:        n.metrics.RPCReceived.Load(),
		CommandsCommitted:  n.metrics.CommandsCommitted.Load(),
		CommandsApplied:    n.metrics.CommandsApplied.Load(),
		HeartbeatsSent:     n.metrics.HeartbeatsSent.Load(),
		HeartbeatsReceived: n.metrics.HeartbeatsReceived.Load(),
		CommitIndex:        commitIndex,
		LastApplied:        lastApplied,
		LogLength:          logLength,
	}
}

// PrintMetrics prints the exact requested output format for a node's metrics.
func (n *Node) PrintMetrics() {
	snap := n.GetMetricsSnapshot()

	fmt.Printf("====================================\n")
	fmt.Printf("Node %d Metrics\n", snap.NodeID)
	fmt.Printf("==============\n\n")

	fmt.Printf("State: %s\n", snap.State)
	fmt.Printf("Current Term: %d\n", snap.CurrentTerm)
	fmt.Printf("Elections Won: %d\n", snap.ElectionsWon)
	fmt.Printf("Elections Lost: %d\n", snap.ElectionsLost)
	fmt.Printf("RPC Sent: %d\n", snap.RPCSent)
	fmt.Printf("RPC Received: %d\n", snap.RPCReceived)
	fmt.Printf("Heartbeats Sent: %d\n", snap.HeartbeatsSent)
	fmt.Printf("Heartbeats Received: %d\n", snap.HeartbeatsReceived)
	fmt.Printf("Commands Committed: %d\n", snap.CommandsCommitted)
	fmt.Printf("Commands Applied: %d\n", snap.CommandsApplied)
	fmt.Printf("Commit Index: %d\n", snap.CommitIndex)
	fmt.Printf("Last Applied: %d\n", snap.LastApplied)
	fmt.Printf("Log Length: %d\n", snap.LogLength)
	fmt.Printf("==============\n")
}
