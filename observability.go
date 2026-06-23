package main

// Observability types for the API layer

type LogEntrySummary struct {
	Index     int    `json:"index"`
	Term      int    `json:"term"`
	Command   string `json:"command"`
	Committed bool   `json:"committed"`
}

type NodeLogSummary struct {
	NodeID  int               `json:"node_id"`
	State   string            `json:"state"`
	Entries []LogEntrySummary `json:"entries"`
}

type NodeSnapshotSummary struct {
	NodeID            int `json:"node_id"`
	LastIncludedIndex int `json:"lastIncludedIndex"`
	LastIncludedTerm  int `json:"lastIncludedTerm"`
	PhysicalLogLength int `json:"physicalLogLength"`
	CompactedEntries  int `json:"compactedEntries"`
}

type NodeStateMachineSummary struct {
	NodeID int               `json:"node_id"`
	KV     map[string]string `json:"kv"`
}

type NodeFullSummary struct {
	ID                        int               `json:"id"`
	State                     string            `json:"state"`
	Term                      int               `json:"term"`
	CommitIndex               int               `json:"commitIndex"`
	LastApplied               int               `json:"lastApplied"`
	LogLength                 int               `json:"logLength"`
	SnapshotLastIncludedIndex int               `json:"snapshotLastIncludedIndex"`
	SnapshotLastIncludedTerm  int               `json:"snapshotLastIncludedTerm"`
	RPCSent                   int               `json:"rpcSent"`
	RPCReceived               int               `json:"rpcReceived"`
	HeartbeatsSent            int               `json:"heartbeatsSent"`
	HeartbeatsReceived        int               `json:"heartbeatsReceived"`
	Entries                   []LogEntrySummary `json:"entries"`
	KV                        map[string]string `json:"kv"`
}

// GetLogSummary safely extracts the current log and commit statuses.
func (n *Node) GetLogSummary() NodeLogSummary {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entries := make([]LogEntrySummary, 0)
	for i, entry := range n.log {
		if i == 0 {
			continue // skip sentinel
		}
		entries = append(entries, LogEntrySummary{
			Index:     entry.Index,
			Term:      entry.Term,
			Command:   entry.Command,
			Committed: entry.Index <= n.commitIndex,
		})
	}

	return NodeLogSummary{
		NodeID:  n.id,
		State:   n.state.String(),
		Entries: entries,
	}
}

// GetSnapshotSummary safely extracts physical log memory properties.
func (n *Node) GetSnapshotSummary() NodeSnapshotSummary {
	n.mu.RLock()
	defer n.mu.RUnlock()

	offset := 0
	term := 0
	if len(n.log) > 0 {
		offset = n.log[0].Index
		term = n.log[0].Term
	}

	return NodeSnapshotSummary{
		NodeID:            n.id,
		LastIncludedIndex: offset,
		LastIncludedTerm:  term,
		PhysicalLogLength: len(n.log) - 1, // Exclude sentinel
		CompactedEntries:  offset,         // The offset represents the number of entries deleted
	}
}

// GetStateMachineSummary safely copies the KV store state.
func (n *Node) GetStateMachineSummary() NodeStateMachineSummary {
	// Snapshot() internally acquires the KVStore's RLock
	kv := n.stateMachine.Snapshot()
	return NodeStateMachineSummary{
		NodeID: n.id,
		KV:     kv,
	}
}

// GetFullSummary returns all internal metrics and states for a deep dive.
func (n *Node) GetFullSummary() NodeFullSummary {
	n.mu.RLock()
	defer n.mu.RUnlock()

	offset := 0
	term := 0
	if len(n.log) > 0 {
		offset = n.log[0].Index
		term = n.log[0].Term
	}

	// Prepare entries copy
	entries := make([]LogEntrySummary, 0)
	for i, entry := range n.log {
		if i == 0 {
			continue
		}
		entries = append(entries, LogEntrySummary{
			Index:     entry.Index,
			Term:      entry.Term,
			Command:   entry.Command,
			Committed: entry.Index <= n.commitIndex,
		})
	}

	return NodeFullSummary{
		ID:                        n.id,
		State:                     n.state.String(),
		Term:                      n.currentTerm,
		CommitIndex:               n.commitIndex,
		LastApplied:               n.lastApplied,
		LogLength:                 len(n.log) - 1,
		SnapshotLastIncludedIndex: offset,
		SnapshotLastIncludedTerm:  term,
		RPCSent:                   int(n.metrics.RPCSent.Load()),
		RPCReceived:               int(n.metrics.RPCReceived.Load()),
		HeartbeatsSent:            int(n.metrics.HeartbeatsSent.Load()),
		HeartbeatsReceived:        int(n.metrics.HeartbeatsReceived.Load()),
		Entries:                   entries,
		KV:                        n.stateMachine.Snapshot(),
	}
}
