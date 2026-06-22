package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
)

// PersistentState holds the subset of a node's state that must be durably
// saved to disk. According to the Raft paper (§5.1), this state must be
// persisted before responding to any RPCs to survive crashes without
// violating safety guarantees.
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// persist safely copies the node's persistent state under a read lock,
// then serializes and writes it to disk without holding the lock.
//
// This method avoids holding n.mu during disk I/O, preventing the node
// from deadlocking or stalling concurrent RPCs.
//
// If writing to disk fails, the error is logged but not fatal.
func (n *Node) persist() error {
	// 1. Acquire read lock and copy the state
	n.mu.RLock()
	state := PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         make([]LogEntry, len(n.log)),
	}
	copy(state.Log, n.log)
	n.mu.RUnlock()

	// 2. Serialize the state to a buffer
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		n.logf("persist failed: failed to encode state: %v", err)
		return err
	}

	// 3. Ensure the data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		n.logf("persist failed: could not create data dir: %v", err)
		return err
	}

	// 4. Write to a temporary file, then do an atomic rename to prevent
	// corrupted states in case of an abrupt crash during writing.
	filename := fmt.Sprintf("data/node-%d.state", n.id)
	tmpFilename := filename + ".tmp"

	if err := os.WriteFile(tmpFilename, buf.Bytes(), 0644); err != nil {
		n.logf("persist failed: could not write temp file: %v", err)
		return err
	}

	if err := os.Rename(tmpFilename, filename); err != nil {
		n.logf("persist failed: could not rename temp file: %v", err)
		return err
	}

	return nil
}

// restore attempts to read the persistent state from disk.
// If the file does not exist, it simply returns (starting with fresh state).
// If it exists, it loads the state into the node.
//
// This is called during node initialization (NewNode) and when simulating
// a crash recovery (Revive).
func (n *Node) restore() error {
	filename := fmt.Sprintf("data/node-%d.state", n.id)
	
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// Normal for a brand new node.
			return nil
		}
		n.logf("restore failed: could not read file: %v", err)
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var state PersistentState
	if err := dec.Decode(&state); err != nil {
		n.logf("restore failed: could not decode state: %v", err)
		return err
	}

	n.mu.Lock()
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
	n.mu.Unlock()

	n.logf("restored state from disk: term=%d, votedFor=%d, logLength=%d",
		state.CurrentTerm, state.VotedFor, len(state.Log))
	
	return nil
}
