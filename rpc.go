// rpc.go — MiniRaft Full Log Replication
//
// This file implements the complete AppendEntries RPC — the workhorse of the
// Raft consensus algorithm. It covers two distinct protocol roles:
//
//   Sender side (leader):
//     SubmitCommand()         — client command ingestion
//     replicateToFollowers()  — fan-out per-peer replication goroutines
//     replicateToFollower()   — per-peer retry loop with nextIndex tracking
//     advanceCommitIndex()    — majority quorum commit calculation
//
//   Receiver side (follower/candidate):
//     AppendEntries()         — full handler: term check, log consistency,
//                               log truncation, log append, commitIndex advance
//     sendAppendEntries()     — transport wrapper (used by heartbeat.go too)
//
// Protocol invariants enforced in this file:
//
//   Log Matching Property (§5.3):
//     If two log entries have the same (index, term), all preceding entries
//     are identical. Enforced by the prevLogIndex/prevLogTerm check in the
//     AppendEntries handler.
//
//   Leader Completeness (§5.4):
//     A newly elected leader has all committed entries. Enforced by the
//     Election Restriction in RequestVote (election.go).
//
//   Leader Append-Only (§5.3):
//     A leader never modifies existing log entries; it only appends.
//     The follower handler may truncate conflicting entries — this is safe
//     because truncated entries are always uncommitted (see §5.4.2).
//
//   Commit Safety (§5.4.2):
//     Only entries from the leader's CURRENT term are directly committed.
//     Entries from old terms are committed indirectly when a current-term
//     entry at a later index is committed.
//
// Concurrency model:
//   All state reads/writes happen under n.mu.
//   Network I/O (RPC calls) NEVER happens while holding n.mu.
//   The pattern throughout: snapshot → release lock → RPC → reacquire lock → process.
//   Stale replies are discarded via (state == Leader && currentTerm == termAtSend) checks.
//
// Raft paper reference: §5.3, §5.4, Figure 2.

package main

import (
	"fmt"
	"os"
)

// ---------------------------------------------------------------------------
// RPC types (used by both heartbeat.go and rpc.go)
// ---------------------------------------------------------------------------

// AppendEntriesArgs contains the arguments for the AppendEntries RPC.
// This struct is used for heartbeats (Entries=[]) and log replication (Entries≠[]).
// Raft paper Figure 2.
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term int

	// LeaderID identifies the leader for client redirection.
	LeaderID int

	// PrevLogIndex is the index of the log entry immediately before the new entries.
	// Follower must have an entry at this index with term == PrevLogTerm.
	// This check enforces the Log Matching Property (§5.3).
	PrevLogIndex int

	// PrevLogTerm is the expected term at PrevLogIndex in the follower's log.
	PrevLogTerm int

	// Entries is the sequence of log entries to append after PrevLogIndex.
	// Empty for heartbeats. One or more for log replication.
	Entries []LogEntry

	// LeaderCommit is the leader's current commitIndex. Followers use this
	// to advance their own commitIndex after appending entries.
	LeaderCommit int
}

// AppendEntriesReply contains the response to an AppendEntries RPC.
// Raft paper Figure 2.
type AppendEntriesReply struct {
	// Term is the follower's currentTerm. The leader uses this to detect
	// if it has become stale (reply.Term > leader.currentTerm → step down).
	Term int

	// Success is true if the follower accepted the AppendEntries.
	// False means either: stale term, or log inconsistency (prevLog mismatch).
	Success bool

	// ConflictTerm is the term of the conflicting entry at PrevLogIndex.
	// Used by the leader for accelerated nextIndex backtracking (§5.3 optimization).
	// 0 means the follower's log was too short (didn't have PrevLogIndex at all).
	ConflictTerm int

	// ConflictIndex is the first index in the log with ConflictTerm.
	// Allows the leader to skip an entire conflicting term in one step instead
	// of decrementing nextIndex one position at a time.
	// When ConflictTerm==0, ConflictIndex is lastLogIndex+1 (log too short).
	ConflictIndex int
}

// ---------------------------------------------------------------------------
// AppendEntries — the RPC handler (receiver/follower side)
// ---------------------------------------------------------------------------

// AppendEntries is the net/rpc handler called by the leader to replicate log
// entries and maintain heartbeat contact. This single handler serves both
// heartbeats (Entries=[]) and log replication (Entries≠[]).
//
// Raft paper reference: §5.2 (heartbeat), §5.3 (log replication), Figure 2.
//
// The handler implements the "AppendEntries RPC Receiver Implementation" from
// Figure 2 verbatim:
//
//  1. Reply false if term < currentTerm (§5.1)
//  2. Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm (§5.3)
//  3. If existing entry conflicts with new one (same index, different terms):
//     delete the existing entry and all that follow it (§5.3)
//  4. Append any new entries not already in the log
//  5. If leaderCommit > commitIndex:
//     set commitIndex = min(leaderCommit, index of last new entry)
//
// Lock discipline:
//   - The write lock is held for all state mutations (rules 1–5).
//   - The lock is released BEFORE resetElectionTimer() — timer operations
//     have their own internal lock; nesting them with n.mu causes deadlocks.
//   - The lock is released BEFORE applyCommitted() — apply involves logging
//     (I/O) and the lock should not be held across I/O operations.
//
// Common bugs in this handler:
//   - Resetting election timer for stale/rejected AppendEntries
//     → Stale leaders suppress valid elections.
//   - Truncating the log unconditionally at PrevLogIndex
//     → Correctly-replicated committed entries get overwritten.
//   - Not transitioning Candidate → Follower on valid same-term AppendEntries
//     → Two "leaders" coexist: one campaigning, one established.
//   - Not setting commitIndex = min(leaderCommit, lastLogIndex)
//     → Followers commit entries they don't yet have.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	// Dead-node guard: reject any RPC if this node has been killed.
	// This handles the race where an RPC arrives just as Kill() is called.
	if n.isDead() {
		return fmt.Errorf("node %d is dead", n.id)
	}

	n.metrics.RPCReceived.Add(1)
	if len(args.Entries) == 0 {
		n.metrics.HeartbeatsReceived.Add(1)
	}

	n.mu.Lock()

	// -----------------------------------------------------------------------
	// Rule 1: Reject AppendEntries from stale leaders. (§5.1)
	//
	// A stale leader is one whose term is less than our currentTerm.
	// We inform it of the higher term so it steps down.
	// We do NOT reset the election timer — the stale leader's presence doesn't
	// mean there's a valid authority; we may need to start our own election.
	// -----------------------------------------------------------------------
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		n.logfLocked("REJECT AppendEntries from Node %d — stale leader (their=%d, ours=%d)",
			args.LeaderID, args.Term, n.currentTerm)
		n.mu.Unlock()
		return nil
	}

	// -----------------------------------------------------------------------
	// Higher-term step-down and same-term Candidate→Follower transition.
	//
	// checkTerm: if args.Term > currentTerm → sets state=Follower, clears votedFor.
	// If args.Term == currentTerm and we're Candidate: another node won this term's
	// election. We revert to Follower WITHOUT clearing votedFor (we voted for
	// ourselves; clearing it would allow double-voting in the same term).
	// -----------------------------------------------------------------------
	// -----------------------------------------------------------------------
	stateChanged := n.checkTerm(args.Term)

	if n.state == Candidate {
		// Same term: another candidate won. Stop campaigning.
		// Note: we set state directly, NOT via becomeFollower(), to preserve votedFor.
		n.state = Follower
		n.logfLocked("CANDIDATE → FOLLOWER (leader Node %d established for term %d)",
			args.LeaderID, args.Term)
	}

	reply.Term = n.currentTerm // always echo our current term back to the leader

	// -----------------------------------------------------------------------
	// Rule 2: Log consistency check — the Log Matching Property. (§5.3)
	//
	// We must have an entry at args.PrevLogIndex with term == args.PrevLogTerm.
	// This ensures that our log agrees with the leader's log up through
	// PrevLogIndex before we append any new entries.
	//
	// Two failure cases:
	//
	// Case A — Our log is too short (we don't have PrevLogIndex at all):
	//   ConflictIndex = lastLogIndex + 1  (where our log ends)
	//   ConflictTerm  = 0                 (signals "log too short")
	//   Leader will set nextIndex[us] = ConflictIndex and retry.
	//
	// Case B — We have PrevLogIndex but with a different term:
	//   ConflictTerm  = our log[PrevLogIndex].Term
	//   ConflictIndex = first index of ConflictTerm in our log
	//   (Optimization: leader can skip all entries of ConflictTerm in one step)
	// -----------------------------------------------------------------------

	// Case A: Our log doesn't reach PrevLogIndex.
	if args.PrevLogIndex > n.lastLogIndex() {
		reply.Success = false
		reply.ConflictIndex = n.lastLogIndex() + 1
		reply.ConflictTerm = 0
		n.logfLocked("REJECT AppendEntries: log too short "+
			"(prevLogIndex=%d, ourLastIndex=%d) — sending conflictIndex=%d",
			args.PrevLogIndex, n.lastLogIndex(), reply.ConflictIndex)
		n.mu.Unlock()
		if stateChanged {
			n.persist()
		}
		// We DO reset the timer: the leader is valid (term check passed),
		// the rejection is about log state, not authority.
		n.resetElectionTimer()
		return nil
	}

	offset := n.log[0].Index

	// Case B: Entry at PrevLogIndex has the wrong term.
	if args.PrevLogIndex < offset {
		reply.Success = false
		reply.ConflictIndex = offset + 1
		reply.ConflictTerm = 0
		n.mu.Unlock()
		return nil
	}

	if args.PrevLogIndex > offset && n.log[args.PrevLogIndex-offset].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex-offset].Term
		reply.Success = false
		reply.ConflictTerm = conflictTerm

		conflictIndex := args.PrevLogIndex
		for conflictIndex > offset+1 && n.log[conflictIndex-1-offset].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex

		n.logfLocked("REJECT AppendEntries: term mismatch at log[%d] "+
			"(expected term=%d, found term=%d) → conflictTerm=%d, conflictIndex=%d",
			args.PrevLogIndex, args.PrevLogTerm, conflictTerm,
			reply.ConflictTerm, reply.ConflictIndex)
		n.mu.Unlock()
		if stateChanged {
			n.persist()
		}
		n.resetElectionTimer()
		return nil
	} else if args.PrevLogIndex == offset && args.PrevLogIndex != 0 {
		if n.log[0].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictIndex = offset
			reply.ConflictTerm = 0
			n.mu.Unlock()
			return nil
		}
	}

	// -----------------------------------------------------------------------
	// Rule 3 & 4: Append new entries, truncating only on actual conflict. (§5.3)
	//
	// CRITICAL: We must NOT unconditionally truncate at PrevLogIndex and
	// re-append. That would destroy committed entries if the leader resends
	// an older suffix. Instead we compare entry-by-entry:
	//
	//   If existing entry at index I has the SAME term as the new entry → SKIP
	//   (Log Matching Property guarantees the commands are also identical)
	//
	//   If existing entry at index I has a DIFFERENT term → TRUNCATE from I
	//   and append the remainder of the leader's entries.
	//
	//   If index I is past our log's end → APPEND.
	//
	// Why is truncation safe? (§5.4.2)
	//   A follower truncates entries only when the leader sends an entry with
	//   the same index but a different term. The leader's version is always
	//   more authoritative. Crucially, truncated entries are always UNCOMMITTED:
	//   if they were committed, a majority would have them, and the leader (which
	//   was elected by majority) would also have them with the same term.
	// -----------------------------------------------------------------------
	for i, entry := range args.Entries {
		logIdx := args.PrevLogIndex + 1 + i

		if logIdx < len(n.log)+offset {
			if n.log[logIdx-offset].Term == entry.Term {
				continue
			}
			n.logfLocked("TRUNCATE log[%d:] — conflict: our term=%d, leader term=%d",
				logIdx, n.log[logIdx-offset].Term, entry.Term)
			n.log = n.log[:logIdx-offset]
			n.log = append(n.log, args.Entries[i:]...)
			stateChanged = true
			break
		}

		n.log = append(n.log, args.Entries[i:]...)
		stateChanged = true
		break
	}

	if len(args.Entries) > 0 {
		n.logfLocked("APPEND %d entries [%d..%d] from leader Node %d",
			len(args.Entries),
			args.Entries[0].Index,
			args.Entries[len(args.Entries)-1].Index,
			args.LeaderID)
	}

	// -----------------------------------------------------------------------
	// Rule 5: Advance commitIndex. (§5.3)
	//
	// If the leader's commitIndex is ahead of ours, we advance ours to:
	//   min(leaderCommit, lastLogIndex)
	//
	// Why min with lastLogIndex?
	//   The leader might have committed entries we don't have yet. We can only
	//   commit up to what we actually have in our log. The remaining entries
	//   will be committed after the leader sends them in the next round.
	//
	// Note: This clause applies to BOTH heartbeats and replication. A heartbeat
	// with LeaderCommit=5 causes the follower to commit entries 1-5 if it has
	// them — even without any new entries arriving.
	// -----------------------------------------------------------------------
	if args.LeaderCommit > n.commitIndex {
		newCommit := minInt(args.LeaderCommit, n.lastLogIndex())
		if newCommit > n.commitIndex {
			n.logfLocked("commitIndex: %d → %d (leaderCommit=%d)",
				n.commitIndex, newCommit, args.LeaderCommit)
			
			n.metrics.CommandsCommitted.Add(int64(newCommit - n.commitIndex))
			
			n.commitIndex = newCommit
		}
	}

	reply.Success = true
	shouldApply := n.lastApplied < n.commitIndex

	n.mu.Unlock()

	if stateChanged {
		n.persist()
	}

	// Reset election timer — we've confirmed a valid leader exists.
	// Done AFTER releasing the lock (timer has its own internal lock).
	n.resetElectionTimer()

	// Apply any newly committed entries to the state machine.
	// Done AFTER releasing the lock (involves I/O).
	if shouldApply {
		n.applyCommitted()
	}

	return nil
}

// ---------------------------------------------------------------------------
// sendAppendEntries — transport wrapper (used by heartbeat.go and rpc.go)
// ---------------------------------------------------------------------------

// sendAppendEntries sends an AppendEntries RPC to peerID.
// Returns true on success, false if the peer is unreachable or RPC fails.
//
// Callers MUST NOT hold n.mu. This is a blocking network operation.
// This function is the shared transport layer for both heartbeat.go (Phase 4)
// and the log replication paths in this file (Phase 5).
func (n *Node) sendAppendEntries(peerID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	n.metrics.RPCSent.Add(1)
	if len(args.Entries) == 0 {
		n.metrics.HeartbeatsSent.Add(1)
	}

	client := n.getClient(peerID)
	if client == nil {
		return false
	}

	err := client.Call("Node.AppendEntries", args, reply)
	if err != nil {
		n.logf("RPC error AppendEntries→Node %d: %v", peerID, err)
		n.closeClient(peerID)
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// SubmitCommand — client-facing command ingestion
// ---------------------------------------------------------------------------

// SubmitCommand is called by a client (or test) to submit a command to the
// Raft cluster. Only the Leader can accept commands.
//
// Returns:
//
//	index    — log index where the command was appended (1-based)
//	term     — current term
//	isLeader — false if this node is not the leader (client should retry
//	            with a different node or wait for a leader to be elected)
//
// Raft paper reference: §5.3
//
//	"The leader appends the command to its log as a new entry, then issues
//	 AppendEntries RPCs in parallel to each of the other servers to replicate
//	 the entry."
//
// On return, the command has been appended to the leader's log but is NOT yet
// committed. It becomes committed when a majority of nodes acknowledge it.
// The caller can detect commitment by polling GetCommitIndex() >= index, or by
// waiting for applyCommitted() to fire (which logs the apply).
//
// Lock discipline:
//
//	SubmitCommand holds n.mu for the append, then releases before spawning
//	replication goroutines. The goroutines acquire n.mu independently.
func (n *Node) SubmitCommand(command string) (index int, term int, isLeader bool) {
	n.mu.Lock()

	if n.state != Leader {
		term = n.currentTerm
		n.mu.Unlock()
		return -1, term, false
	}

	// Append the new entry to the leader's local log.
	// The leader's log is the source of truth — all followers must eventually
	// replicate exactly this sequence.
	index = n.lastLogIndex() + 1
	term = n.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	n.log = append(n.log, entry)

	n.logfLocked("▶ CLIENT command accepted: log[%d]={term=%d, cmd=%q} — replicating to %d peers",
		index, term, command, len(n.peers))

	// Capture term for the replication goroutines' stale-reply guard.
	replicationTerm := n.currentTerm
	n.mu.Unlock()

	n.persist()

	// Trigger immediate replication — don't wait for the next heartbeat tick.
	// This is what makes the commit latency ~1 network RTT rather than ~50ms.
	go n.replicateToFollowers(replicationTerm)

	return index, term, true
}

// ---------------------------------------------------------------------------
// replicateToFollowers — fan-out replication
// ---------------------------------------------------------------------------

// replicateToFollowers fans out one replication goroutine per peer.
// Each goroutine independently manages its retry loop for that peer.
//
// Why one goroutine per peer?
//
//	A slow or unreachable peer must not block replication to other peers.
//	In a 3-node cluster with one crashed follower, the other follower must
//	still receive entries and allow commits (quorum = 2).
//
// The term parameter is passed to each goroutine as a stale-reply guard.
// If n.currentTerm advances beyond term, the goroutine exits.
func (n *Node) replicateToFollowers(term int) {
	for peerID := range n.peers {
		go n.replicateToFollower(peerID, term)
	}
}

// ---------------------------------------------------------------------------
// replicateToFollower — per-peer retry loop
// ---------------------------------------------------------------------------

// replicateToFollower runs the per-peer log replication loop for peerID.
// It retries AppendEntries until:
//
//	a) The follower is fully caught up (success, no more entries to send)
//	b) The peer is unreachable (network failure — retry deferred to heartbeat)
//	c) A higher term is observed (leader steps down)
//	d) The node is no longer leader in the same term (state changed)
//	e) The node is killed (isDead())
//
// Raft paper reference: §5.3 (log replication), Figure 2 (leader rules)
//
// nextIndex backtracking:
//
//	On a rejection, the follower sends ConflictTerm and ConflictIndex to
//	enable accelerated backtracking. The leader searches its own log for
//	the last entry with ConflictTerm:
//
//	Case 1: Leader has entries with ConflictTerm:
//	  nextIndex = (last leader index with ConflictTerm) + 1
//	  Rationale: the conflict is within a term the leader knows about.
//	  Set nextIndex to just after the last good position.
//
//	Case 2: Leader has NO entries with ConflictTerm:
//	  nextIndex = ConflictIndex
//	  Rationale: the follower has entries the leader doesn't recognize.
//	  Jump to the start of the unknown region.
//
//	Case 3: ConflictTerm == 0 (log too short):
//	  nextIndex = ConflictIndex (= follower's lastLogIndex + 1)
//	  Rationale: skip directly to where the follower's log ends.
//
// Without this optimization (simple nextIndex--), synchronizing a follower
// that is N entries behind requires N round trips. With this optimization,
// it requires at most O(number of distinct terms) round trips.
//
// Common bugs in this function:
//   - Not checking stale-reply guards after reacquiring lock.
//   - Using nextIndex for commitIndex calculation (must use matchIndex).
//   - Decrementing nextIndex below 1 (causes out-of-bounds panic on log read).
//   - Not updating matchIndex before calling advanceCommitIndex.
//   - Forgetting to call applyCommitted after advancing commitIndex.
func (n *Node) replicateToFollower(peerID int, term int) {
	for !n.isDead() {
		// =================================================================
		// STEP 1: Build AppendEntriesArgs under lock
		// =================================================================
		n.mu.Lock()

		// Stale guard: are we still the leader in the correct term?
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		// Read replication state for this peer.
		nextIdx := n.nextIndex[peerID]
		offset := n.log[0].Index

		if nextIdx <= offset {
			// Peer is too far behind; we must send a snapshot instead of AppendEntries.
			args := &InstallSnapshotArgs{
				Term:              n.currentTerm,
				LeaderID:          n.id,
				LastIncludedIndex: offset,
				LastIncludedTerm:  n.log[0].Term,
			}
			
			// We need to read the snapshot file from disk without holding the lock.
			n.mu.Unlock()
			
			snapData, err := os.ReadFile(fmt.Sprintf("data/node-%d.snapshot", n.id))
			if err != nil {
				n.logf("failed to read snapshot file for peer %d: %v", peerID, err)
				return
			}
			args.Data = snapData
			
			// We execute InstallSnapshot instead
			go n.sendInstallSnapshot(peerID, args)
			return
		}

		prevLogIndex := nextIdx - 1
		prevLogTerm := n.getLogTerm(prevLogIndex)

		// Collect all entries from nextIdx to the end of the leader's log.
		var entries []LogEntry
		if nextIdx <= n.lastLogIndex() {
			raw := n.log[nextIdx-offset:]
			entries = make([]LogEntry, len(raw))
			copy(entries, raw)
		}

		args := &AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		n.mu.Unlock()

		// =================================================================
		// STEP 2: If caught up, nothing to replicate — exit the loop.
		// =================================================================
		// The heartbeat ticker handles keep-alive; we only retry here if
		// there are actual entries to send.
		if len(entries) == 0 {
			return
		}

		n.logf("→ AppendEntries to Node %d: prevLogIndex=%d prevLogTerm=%d entries=%d..%d",
			peerID, prevLogIndex, prevLogTerm,
			args.Entries[0].Index,
			args.Entries[len(args.Entries)-1].Index)

		// =================================================================
		// STEP 3: Send RPC — no lock held
		// =================================================================
		reply := &AppendEntriesReply{}
		ok := n.sendAppendEntries(peerID, args, reply)
		if !ok {
			// Peer is unreachable. Return — the heartbeat ticker will retry
			// on the next 50ms interval. We do not spin here, because spinning
			// against a crashed peer would waste CPU and delay other work.
			return
		}

		// =================================================================
		// STEP 4: Process reply under write lock
		// =================================================================
		n.mu.Lock()

		// Stale-reply guard: verify we are still the leader in the same term.
		// The state machine may have changed while we were waiting for the RPC.
		if n.state != Leader || n.currentTerm != term {
			n.logfLocked("discard AppendEntries reply from Node %d — state changed "+
				"(state=%s, term=%d, termAtSend=%d)", peerID, n.state, n.currentTerm, term)
			n.mu.Unlock()
			return
		}

		// Higher-term step-down: follower's currentTerm > ours.
		// §5.1: any RPC response with Term T > currentTerm → step down.
		if reply.Term > n.currentTerm {
			n.logfLocked("💔 LEADER→FOLLOWER: Node %d replied term=%d (our term=%d)",
				peerID, reply.Term, n.currentTerm)
			n.checkTerm(reply.Term)
			n.mu.Unlock()
			n.persist()
			n.resetElectionTimer()
			return
		}

		if !reply.Success {
			// Log inconsistency — apply accelerated nextIndex backtracking.
			// (Stale-leader rejection is now caught above via reply.Term check.)
			//
			// Sub-case A: ConflictIndex==0 && ConflictTerm==0 is no longer used
			// for stale-leader detection. Any success=false here means log mismatch.

			// Sub-case B: Log inconsistency — update nextIndex and retry.
			//
			// Accelerated backtracking algorithm:
			//   1. Search our log for the last entry with reply.ConflictTerm.
			//   2. If found: nextIndex = (last index with ConflictTerm) + 1
			//      This skips past the entire ConflictTerm region.
			//   3. If not found: nextIndex = reply.ConflictIndex
			//      Jump directly to where the follower says the conflict starts.
			//   4. ConflictTerm==0 (log too short): nextIndex = ConflictIndex
			//      Jump to where the follower's log ends.

			if reply.ConflictIndex == 0 && reply.ConflictTerm == 0 {
				// Unexpected: no conflict info provided. Simple decrement.
				n.nextIndex[peerID] = maxInt(1, n.nextIndex[peerID]-1)
				n.logfLocked("backtrack Node %d: no conflict info → nextIndex=%d",
					peerID, n.nextIndex[peerID])
			} else if reply.ConflictTerm != 0 {
				// Find the last index in OUR log with ConflictTerm.
				lastWithConflictTerm := -1
				offset := n.log[0].Index
				for i := n.lastLogIndex(); i > offset; i-- {
					if n.log[i-offset].Term == reply.ConflictTerm {
						lastWithConflictTerm = i
						break
					}
				}

				if lastWithConflictTerm != -1 {
					// We have entries with ConflictTerm — set nextIndex past them.
					n.nextIndex[peerID] = lastWithConflictTerm + 1
					n.logfLocked("backtrack Node %d: found conflictTerm=%d at index=%d → nextIndex=%d",
						peerID, reply.ConflictTerm, lastWithConflictTerm,
						n.nextIndex[peerID])
				} else {
					// We don't have ConflictTerm — jump to ConflictIndex.
					n.nextIndex[peerID] = reply.ConflictIndex
					n.logfLocked("backtrack Node %d: conflictTerm=%d not in our log → nextIndex=%d",
						peerID, reply.ConflictTerm, n.nextIndex[peerID])
				}
			} else if reply.ConflictIndex > 0 {
				// ConflictTerm == 0: follower's log is too short.
				// nextIndex = where the follower's log ends.
				n.nextIndex[peerID] = reply.ConflictIndex
				n.logfLocked("backtrack Node %d: log too short → nextIndex=%d",
					peerID, n.nextIndex[peerID])
			}

			// Safety clamp: nextIndex must never go below 1.
			// Sending entries starting at index 0 would include the sentinel
			// entry, which must never be sent.
			if n.nextIndex[peerID] < 1 {
				n.nextIndex[peerID] = 1
			}

			n.mu.Unlock()
			// Loop continues immediately — retry with decremented nextIndex.
			continue
		}

		// =================================================================
		// STEP 5: Success — advance matchIndex, nextIndex, and commitIndex
		// =================================================================
		//
		// matchIndex[peerID] = last index of the entries we just sent.
		// This is PrevLogIndex (what was there before) + count of new entries.
		//
		// We use max() to ensure matchIndex never decreases. This handles the
		// race where two concurrent goroutines both process success replies:
		//   Goroutine A processed entries 1-3: matchIndex should go to 3
		//   Goroutine B processed entries 1-5: matchIndex should go to 5
		//   If B runs first and sets matchIndex=5, A must not set it back to 3.
		newMatchIndex := prevLogIndex + len(entries)
		if newMatchIndex > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatchIndex
			n.logfLocked("✓ Node %d replicated log[1..%d] — matchIndex=%d nextIndex=%d",
				peerID, newMatchIndex, n.matchIndex[peerID], newMatchIndex+1)
		}
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1

		// Attempt to advance commitIndex based on the updated matchIndex values.
		// This is the quorum commit check — see advanceCommitIndex() for details.
		n.advanceCommitIndex()
		shouldApply := n.lastApplied < n.commitIndex

		n.mu.Unlock()

		// Apply committed entries outside the lock.
		if shouldApply {
			n.applyCommitted()
		}

		// Success — follower is now up to date.
		// If there are no more entries after this batch, the next iteration
		// will find len(entries)==0 and exit. If new entries were added by
		// another SubmitCommand during this RPC, the loop continues and sends them.
		// (In practice: the new SubmitCommand spawns a fresh goroutine, so we exit.)
		return
	}
}

// ---------------------------------------------------------------------------
// advanceCommitIndex — majority quorum commit calculation
// ---------------------------------------------------------------------------

// advanceCommitIndex checks whether any new log entries can be committed and
// advances n.commitIndex to the highest such entry.
//
// Raft paper reference: §5.3 (commitment), §5.4.2 (only commit currentTerm entries)
//
// Commit rules (all must hold for index N to be committed):
//
//	Rule 1: N > n.commitIndex
//	  Commitment is monotone — we only advance, never retract.
//
//	Rule 2: n.log[N].Term == n.currentTerm
//	  THE MOST IMPORTANT RULE. Leaders may only directly commit entries
//	  from their own current term. Entries from previous terms are committed
//	  only indirectly, when a current-term entry at a later index is committed.
//
//	  Why? (Figure 8 scenario):
//	    Leader A (term=1) replicates entry at index 2 to itself and follower B.
//	    Leader A crashes. Leader C (term=2) is elected with a different entry
//	    at index 2 (from a different partition).
//	    If Leader A's term-1 entry was directly committed, Leader C's election
//	    would be blocked by the election restriction. But if it wasn't committed
//	    directly, Leader C could overwrite it — which is safe only because the
//	    overwritten entry was never committed.
//
//	    The fix: Leader A can only commit the term-1 entry by first appending
//	    a term-1 entry (new command) and committing THAT. The Log Matching
//	    Property then commits all prior entries transitively.
//
//	Rule 3: A majority of nodes have matchIndex[i] >= N
//	  "Majority" = len(peers)/2 + 1 (including self via the +1 in quorumSize).
//	  matchIndex is used (not nextIndex) because matchIndex is confirmed.
//
// Algorithm: scan from lastLogIndex down to commitIndex+1, find the highest N
// satisfying all three rules. Break after finding it (commitIndex only advances).
//
// Callers MUST hold n.mu (write lock) before calling this method.
func (n *Node) advanceCommitIndex() {
	// Scan from the newest possible commit candidate down to current+1.
	// We scan downward because we want the HIGHEST N, and once we find it,
	// we're done (all lower N are also committable by the Log Matching Property,
	// but we only update commitIndex once to the highest valid N).
	for N := n.lastLogIndex(); N > n.commitIndex; N-- {

		// Rule 2: Only commit entries from the current term.
		// This is the election restriction applied to commitment.
		if n.getLogTerm(N) != n.currentTerm {
			// Entry at N is from a previous term. Skip.
			// It will be committed indirectly when a currentTerm entry is committed.
			continue
		}

		// Rule 3: Count nodes that have replicated log entry N.
		// Start at 1 for the leader itself (the leader always has its own entries).
		// n.matchIndex is keyed by peerID (not including self), so we count self separately.
		replicationCount := 1 // self
		for _, peerMatchIndex := range n.matchIndex {
			if peerMatchIndex >= N {
				replicationCount++
			}
		}

		offset := n.log[0].Index
		if replicationCount >= n.quorumSize() {
			// Rules 1, 2, 3 all satisfied — commit!
			n.logfLocked("🎯 COMMIT log[%d] cmd=%q term=%d — replicated on %d/%d nodes",
				N, n.log[N-offset].Command, n.currentTerm,
				replicationCount, len(n.peers)+1)
			
			n.metrics.CommandsCommitted.Add(int64(N - n.commitIndex))
			
			n.commitIndex = N
			// Stop: we found the highest committable entry.
			// Lower entries are transitively committed via Log Matching.
			break
		}
	}
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a peer.
func (n *Node) sendInstallSnapshot(peerID int, args *InstallSnapshotArgs) {
	n.mu.RLock()
	client := n.rpcClients[peerID]
	n.mu.RUnlock()
	
	if client == nil {
		return
	}

	var reply InstallSnapshotReply
	err := client.Call("Node.InstallSnapshot", args, &reply)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != args.Term {
		return
	}

	if reply.Term > n.currentTerm {
		n.checkTerm(reply.Term)
		n.state = Follower
		n.persist()
		n.resetElectionTimer()
		return
	}

	// Update matchIndex and nextIndex
	if args.LastIncludedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = args.LastIncludedIndex
		n.nextIndex[peerID] = args.LastIncludedIndex + 1
	}
}
