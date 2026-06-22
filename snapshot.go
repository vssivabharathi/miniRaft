package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
)

const SnapshotThreshold = 100

// Snapshot represents the durable snapshot state as defined in Raft §7.
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	KVState           map[string]string
}

// InstallSnapshotArgs represents the InstallSnapshot RPC payload.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // Encoded Snapshot
}

// InstallSnapshotReply represents the response to InstallSnapshot.
type InstallSnapshotReply struct {
	Term int
}

// checkSnapshotThreshold checks if the log has grown beyond the threshold.
// If it has, it initiates the snapshot creation process.
// Callers MUST hold n.mu (write lock) before calling this method.
func (n *Node) checkSnapshotThreshold() {
	if len(n.log) <= SnapshotThreshold {
		return
	}

	// We take a snapshot up to lastApplied.
	lastIncludedIndex := n.lastApplied
	lastIncludedTerm := n.getLogTerm(lastIncludedIndex)

	// Let's defer snapshot execution to a goroutine to not block the current flow.
	if n.isDead() || n.snapshotInProgress {
		return
	}
	n.snapshotInProgress = true
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() {
			n.mu.Lock()
			n.snapshotInProgress = false
			n.mu.Unlock()
		}()
		n.takeSnapshot(lastIncludedIndex, lastIncludedTerm)
	}()
}

// takeSnapshot handles the multi-step snapshot process.
func (n *Node) takeSnapshot(lastIncludedIndex, lastIncludedTerm int) {
	if n.isDead() {
		return
	}
	// Step 1: Deep copy KV state without holding Node.mu.
	// KVStore.Snapshot() internally uses RLock().
	kvState := n.stateMachine.Snapshot()

	if n.isDead() {
		return
	}

	snap := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		KVState:           kvState,
	}

	// Serialize snapshot.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snap); err != nil {
		n.logf("snapshot failed: encode error: %v", err)
		return
	}

	if n.isDead() {
		return
	}

	// Write to disk without holding any locks.
	if err := os.MkdirAll("data", 0755); err != nil {
		n.logf("snapshot failed: mkdir error: %v", err)
		return
	}
	filename := fmt.Sprintf("data/node-%d.snapshot", n.id)
	tmpFilename := filename + ".tmp"
	if err := os.WriteFile(tmpFilename, buf.Bytes(), 0644); err != nil {
		n.logf("snapshot failed: write error: %v", err)
		return
	}
	if n.isDead() {
		os.Remove(tmpFilename)
		return
	}
	if err := os.Rename(tmpFilename, filename); err != nil {
		n.logf("snapshot failed: rename error: %v", err)
		return
	}

	if n.isDead() {
		return
	}

	// Step 2: Acquire Node.mu and truncate the log.
	n.mu.Lock()

	// Since we released the lock, check if the log was already compacted further.
	if lastIncludedIndex <= n.log[0].Index {
		n.mu.Unlock()
		return // Already compacted beyond this point.
	}

	if n.isDead() {
		n.mu.Unlock()
		return
	}

	n.logfLocked("compacting log up to index %d (term %d)", lastIncludedIndex, lastIncludedTerm)

	// Create new log with sentinel at index 0.
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{
		Index: lastIncludedIndex,
		Term:  lastIncludedTerm,
	}

	// Append remaining log entries.
	oldOffset := n.log[0].Index
	sliceIndex := lastIncludedIndex - oldOffset
	if sliceIndex < len(n.log) {
		newLog = append(newLog, n.log[sliceIndex+1:]...)
	}

	n.log = newLog
	n.mu.Unlock()
	
	if n.isDead() {
		return
	}
	// Persist the new state (which now contains the truncated log).
	n.persist()
}

// restoreSnapshot reads the snapshot from disk on startup.
func (n *Node) restoreSnapshot() error {
	filename := fmt.Sprintf("data/node-%d.snapshot", n.id)
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		n.logf("restoreSnapshot failed: read error: %v", err)
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var snap Snapshot
	if err := dec.Decode(&snap); err != nil {
		n.logf("restoreSnapshot failed: decode error: %v", err)
		return err
	}

	// Apply KVState.
	n.stateMachine.Restore(snap.KVState)

	n.mu.Lock()
	// Update lastApplied and commitIndex if snapshot is ahead.
	if snap.LastIncludedIndex > n.lastApplied {
		n.lastApplied = snap.LastIncludedIndex
	}
	if snap.LastIncludedIndex > n.commitIndex {
		n.commitIndex = snap.LastIncludedIndex
	}
	n.mu.Unlock()

	n.logf("restored snapshot from disk: lastIncludedIndex=%d", snap.LastIncludedIndex)
	return nil
}

// InstallSnapshot handles the InstallSnapshot RPC from the leader.
func (n *Node) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	n.mu.Lock()

	reply.Term = n.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < n.currentTerm {
		n.logfLocked("REJECT InstallSnapshot from Node %d — stale leader (their=%d, ours=%d)",
			args.LeaderID, args.Term, n.currentTerm)
		n.mu.Unlock()
		return nil
	}

	stateChanged := n.checkTerm(args.Term)

	if n.state == Candidate {
		n.state = Follower
		n.logfLocked("CANDIDATE → FOLLOWER (InstallSnapshot from Node %d for term %d)",
			args.LeaderID, args.Term)
	}

	reply.Term = n.currentTerm

	n.resetElectionTimer()

	// Check if this snapshot is older than what we already have.
	if args.LastIncludedIndex <= n.commitIndex {
		n.logfLocked("IGNORE InstallSnapshot — older than commitIndex (snap=%d, commit=%d)",
			args.LastIncludedIndex, n.commitIndex)
		n.mu.Unlock()
		if stateChanged {
			n.persist()
		}
		return nil
	}

	n.logfLocked("INSTALLING SNAPSHOT: lastIncludedIndex=%d, lastIncludedTerm=%d",
		args.LastIncludedIndex, args.LastIncludedTerm)

	// 5. Save snapshot file, discard any existing or partial snapshot.
	// Since we need to write to disk, we must release the lock.
	// Wait, Raft says "Save snapshot file, discard any existing or partial snapshot with a smaller index"
	// To avoid blocking RPC, we can write the snapshot asynchronously, or quickly synchronously.
	// We'll write synchronously because the test expects instantaneous visibility, but without holding mu.
	
	// Copy args to local variables before releasing lock.
	lastIdx := args.LastIncludedIndex
	lastTerm := args.LastIncludedTerm
	snapData := args.Data
	n.mu.Unlock()

	// Write to disk
	if err := os.MkdirAll("data", 0755); err == nil {
		filename := fmt.Sprintf("data/node-%d.snapshot", n.id)
		tmpFilename := filename + ".tmp"
		if err := os.WriteFile(tmpFilename, snapData, 0644); err == nil {
			os.Rename(tmpFilename, filename)
		}
	}

	// Parse snapshot to apply to KVStore
	buf := bytes.NewBuffer(snapData)
	dec := gob.NewDecoder(buf)
	var snap Snapshot
	if err := dec.Decode(&snap); err == nil {
		n.stateMachine.Restore(snap.KVState)
	} else {
		n.logf("InstallSnapshot: failed to decode snapshot data: %v", err)
	}

	n.mu.Lock()
	defer func() {
		n.mu.Unlock()
		n.persist() // Save new log state after unlocking.
	}()

	if lastIdx <= n.commitIndex {
		n.logfLocked("IGNORE InstallSnapshot — became obsolete during disk I/O (snap=%d, commit=%d)", lastIdx, n.commitIndex)
		return nil
	}

	// 6. If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	offset := n.log[0].Index
	if lastIdx >= offset && lastIdx-offset < len(n.log) && n.log[lastIdx-offset].Term == lastTerm {
		// Retain suffix
		newLog := make([]LogEntry, 1)
		newLog[0] = LogEntry{Index: lastIdx, Term: lastTerm}
		newLog = append(newLog, n.log[lastIdx-offset+1:]...)
		n.log = newLog
	} else {
		// 7. Discard the entire log
		n.log = []LogEntry{{Index: lastIdx, Term: lastTerm}}
	}

	// 8. Reset state machine using snapshot contents (already done above)

	// Update volatile state
	if lastIdx > n.commitIndex {
		n.commitIndex = lastIdx
	}
	if lastIdx > n.lastApplied {
		n.lastApplied = lastIdx
	}

	return nil
}
