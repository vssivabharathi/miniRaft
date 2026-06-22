// heartbeat.go — MiniRaft Leader Heartbeat Subsystem
//
// This file implements the leader's heartbeat mechanism. In Raft, heartbeats
// are AppendEntries RPCs with an empty Entries slice. They serve two purposes:
//
//   1. Authority maintenance — tell followers "I am alive and I am your leader,
//      do not start an election."
//
//   2. Commit propagation — the leaderCommit field in the heartbeat lets
//      followers advance their commitIndex even without new log entries.
//
// Raft paper reference: §5.2
//   "The leader sends periodic heartbeats (AppendEntries RPCs that carry no log
//    entries) to all followers in order to maintain its authority and prevent
//    new elections."
//
// Why 50ms heartbeat interval?
//   The election timeout range is 150–300ms. A 50ms heartbeat interval means
//   followers receive ~3–6 heartbeats per timeout window. Even if 2-3 are lost
//   due to network jitter, the remaining ones prevent a spurious election.
//   The ratio of (heartbeat interval : min election timeout) should be at least
//   1:3 in practice. Our ratio is 1:3 = 50ms : 150ms.
//
// Goroutine lifecycle:
//   sendHeartbeats() is launched with "go n.sendHeartbeats()" by startElection()
//   in election.go immediately after becomeLeader(). It runs until:
//     a) The node is killed (isDead() returns true).
//     b) The node steps down from Leader to Follower (state or term changes).
//
//   Each call to sendHeartbeats() captures termWon (the term in which this node
//   was elected). If n.currentTerm ever exceeds termWon, the goroutine exits.
//   This prevents stale heartbeat goroutines from running after a step-down and
//   re-election cycle.
//
// Concurrency model:
//   sendHeartbeats()     — reads state/term under RLock; decides whether to continue
//   heartbeatTicker()    — reads leader state under RLock; fans out goroutines
//   sendHeartbeatToOne() — reads args under RLock; sends RPC without lock;
//                          processes reply under Lock
//
//   The lock is NEVER held across network I/O.
//   Reply processing happens under the write lock with stale-reply guards.

package main

import (
	"fmt"
	"os"
	"time"
)

// ---------------------------------------------------------------------------
// sendHeartbeats — the leader's heartbeat goroutine
// ---------------------------------------------------------------------------

// sendHeartbeats is the long-running leader heartbeat goroutine. It is launched
// immediately after a node wins a leader election (see election.go, startElection).
//
// Raft paper reference: §5.2
//
//	"Leaders send periodic heartbeats to all followers to maintain authority."
//
// Design: one goroutine per leadership epoch.
//
//	When a node becomes leader, one sendHeartbeats() goroutine is started. It
//	captures the term at election time (termWon). The goroutine exits cleanly
//	when:
//	  1. n.isDead() == true    → node crashed
//	  2. n.state != Leader     → stepped down (saw higher term)
//	  3. n.currentTerm != termWon → term advanced (stale leadership epoch)
//
//	Why capture termWon?
//	Without it, a node could: become leader (term=3) → step down → become
//	leader again (term=4). Both epochs would have their own sendHeartbeats()
//	goroutine running. The first goroutine should stop when term changes to 4.
//	termWon == 3 in the first goroutine; it exits when n.currentTerm == 4.
//
// First heartbeat:
//
//	Sent immediately (before the first ticker tick). This is critical: the
//	election took up to 300ms (full timeout window). Without an immediate
//	heartbeat, followers wait another 50ms before learning of the new leader,
//	increasing the window where a spurious election could start.
//
// Ticker vs Sleep:
//
//	We use time.NewTicker (not time.Sleep) because:
//	  - Ticker fires at fixed intervals regardless of how long heartbeat sending takes.
//	  - Sleep would add heartbeat-send duration on top of the interval, causing drift.
//	  - With concurrent per-peer goroutines, sends complete in ~network RTT, which
//	    is negligible — but Ticker is correct-by-construction.
//
// Common bugs:
//   - Missing termWon guard → stale goroutine sends heartbeats in wrong term.
//   - Not sending the first heartbeat immediately → gap before first 50ms tick.
//   - Sending heartbeats sequentially (not concurrently) → blocked on slow peer.
func (n *Node) sendHeartbeats() {
	// Capture the term in which we were elected leader.
	// This goroutine is only valid for leadership in termWon.
	// If currentTerm ever exceeds termWon, we have stepped down and back up,
	// and a new heartbeat goroutine exists for the new epoch.
	n.mu.RLock()
	termWon := n.currentTerm
	n.mu.RUnlock()

	n.logf("💓 heartbeat goroutine started (termWon=%d)", termWon)

	// Send the FIRST heartbeat immediately — do not wait for the first tick.
	// This suppresses follower election timers right after the election completes.
	n.heartbeatTicker(termWon)

	// Start a ticker for subsequent heartbeats.
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Timer fired — check if we are still the leader for this term.
			if n.isDead() {
				n.logf("💓 heartbeat goroutine exiting — node killed (term=%d)", termWon)
				return
			}

			n.mu.RLock()
			state := n.state
			currentTerm := n.currentTerm
			n.mu.RUnlock()

			// Exit condition 1: no longer Leader.
			if state != Leader {
				n.logf("💓 heartbeat goroutine exiting — stepped down from Leader (state=%s, term=%d)",
					state, currentTerm)
				return
			}

			// Exit condition 2: term advanced beyond our leadership epoch.
			// This should not happen without also failing condition 1
			// (becomeFollower updates both state and term), but we guard
			// against it defensively — belt and suspenders.
			if currentTerm != termWon {
				n.logf("💓 heartbeat goroutine exiting — term changed (was=%d, now=%d)",
					termWon, currentTerm)
				return
			}

			// Still leader in same term — send the next round of heartbeats.
			n.heartbeatTicker(termWon)
		}
	}
}

// ---------------------------------------------------------------------------
// heartbeatTicker — one round of concurrent heartbeats
// ---------------------------------------------------------------------------

// heartbeatTicker dispatches one round of AppendEntries heartbeats to all peers
// concurrently. It is called by sendHeartbeats() on each ticker fire.
//
// Each peer gets its own goroutine so that a slow or unreachable peer does not
// block heartbeats to other peers. In a cluster with one crashed follower, the
// remaining healthy follower must still receive timely heartbeats.
//
// Concurrency note:
//
//	This function itself does NOT hold n.mu. It only reads n.peers (immutable
//	after construction, no lock needed) to determine peer IDs.
//	Each spawned goroutine (sendHeartbeatToOne) manages its own lock discipline.
//
// Why a separate function from sendHeartbeats?
//
//	Testability and separation of concerns. A test can call heartbeatTicker
//	directly to trigger exactly one round without the timing complexity of the
//	ticker loop. This also matches how production Raft libraries (etcd) separate
//	the tick-counting logic from the per-peer send logic.
func (n *Node) heartbeatTicker(term int) {
	for peerID := range n.peers {
		// Each peer gets a goroutine. Pass peerID as a function argument —
		// NOT as a closure variable — to avoid the loop-variable capture bug.
		go n.sendHeartbeatToOne(peerID, term)
	}
}

// ---------------------------------------------------------------------------
// sendHeartbeatToOne — send AppendEntries heartbeat to a single peer
// ---------------------------------------------------------------------------

// sendHeartbeatToOne sends one AppendEntries RPC (heartbeat, Entries=[]) to
// the specified peer and processes the reply.
//
// Raft paper reference: §5.2, Figure 2 (AppendEntries RPC sender rules).
//
// Protocol:
//  1. (RLock) Validate we're still the leader; snapshot args.
//  2. (no lock) Send AppendEntries RPC — may block on network.
//  3. (Lock) Process reply with stale-reply guards.
//
// Step-down on higher term:
//
//	§5.1: "If a server receives a response with term T > currentTerm:
//	       set currentTerm = T, convert to follower."
//	The leader discovers it is stale when a follower's reply.Term exceeds its
//	own currentTerm. This happens after a network partition: a new election
//	happened in the isolated partition, producing a higher-term leader. When
//	the partition heals, the old leader receives heartbeat replies with the
//	new higher term and immediately steps down.
//
// Why snapshot args under RLock (not Lock)?
//
//	We only need to READ state for building args. Multiple concurrent
//	sendHeartbeatToOne goroutines can read simultaneously under RLock.
//	Using a write lock here would serialize the heartbeat fan-out, which is
//	counterproductive — concurrent reads are safe and fast.
//
// Why NOT hold the lock during RPC?
//
//	The RPC call may block for an arbitrary duration (network timeout, peer CPU
//	scheduling, etc.). Any other goroutine trying to acquire n.mu during that
//	time would be blocked too — including the RPC server goroutines that need
//	n.mu to service INCOMING AppendEntries/RequestVote RPCs. This deadlock
//	would freeze the entire node.
//
// steppedDown flag pattern:
//
//	After processing the reply under Lock, we set a local bool if we stepped down.
//	We release the lock THEN call resetElectionTimer(). This avoids holding n.mu
//	during timer operations (timer has its own internal lock; nesting locks
//	risks deadlock with other goroutines touching the same timer).
//
// Common bugs:
//   - Sending stale args from a previous term (snapshot-args guard prevents this).
//   - Missing reply.Term check (stale leader remains active in old partition).
//   - Missing state guard after reply (processes reply after stepping down)
func (n *Node) sendHeartbeatToOne(peerID int, term int) {
	// =========================================================================
	// STEP 1: Snapshot args under read-lock
	// =========================================================================
	n.mu.RLock()

	// Stale-send guard: are we still the leader in the correct term?
	if n.state != Leader || n.currentTerm != term {
		n.mu.RUnlock()
		return
	}

	// Build args. The heartbeat sends Entries=[] but ALSO carries entries
	// when the follower is behind (nextIndex[peerID] <= lastLogIndex).
	// This unifies the heartbeat and replication paths: every 50ms tick
	// attempts to bring the follower up to date, not just keep it alive.
	offset := n.log[0].Index
	if n.nextIndex[peerID] <= offset {
		// Needs snapshot
		args := &InstallSnapshotArgs{
			Term:              n.currentTerm,
			LeaderID:          n.id,
			LastIncludedIndex: offset,
			LastIncludedTerm:  n.log[0].Term,
		}
		n.mu.RUnlock()
		
		snapData, err := os.ReadFile(fmt.Sprintf("data/node-%d.snapshot", n.id))
		if err != nil {
			n.logf("failed to read snapshot file for peer %d: %v", peerID, err)
			return
		}
		args.Data = snapData
		
		go n.sendInstallSnapshot(peerID, args)
		return
	}

	prevLogIndex := n.nextIndex[peerID] - 1
	prevLogTerm := n.getLogTerm(prevLogIndex)

	var entries []LogEntry
	if n.nextIndex[peerID] <= n.lastLogIndex() {
		// Follower is behind — include entries in this heartbeat round.
		raw := n.log[n.nextIndex[peerID]-offset:]
		entries = make([]LogEntry, len(raw))
		copy(entries, raw)
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries, // empty if caught up, non-empty if behind
		LeaderCommit: n.commitIndex,
	}

	n.mu.RUnlock()

	// =========================================================================
	// STEP 2: Send the RPC — no lock held
	// =========================================================================
	reply := &AppendEntriesReply{}
	ok := n.sendAppendEntries(peerID, args, reply)
	if !ok {
		return // peer unreachable; retry on next tick
	}

	// =========================================================================
	// STEP 3: Process reply under write-lock
	// =========================================================================
	n.mu.Lock()

	// Stale-reply guard.
	if n.state != Leader || n.currentTerm != term {
		n.logfLocked("discard heartbeat reply from Node %d — state changed (state=%s, term=%d)",
			peerID, n.state, n.currentTerm)
		n.mu.Unlock()
		return
	}

	// Higher-term step-down (§5.1).
	if reply.Term > n.currentTerm {
		n.logfLocked("💔 LEADER → FOLLOWER: Node %d replied term=%d > our term=%d",
			peerID, reply.Term, n.currentTerm)
		n.checkTerm(reply.Term)
		n.mu.Unlock()
		n.persist()
		n.resetElectionTimer()
		n.logf("⬇  stepped down to Follower — election timer reset")
		return
	}

	if reply.Success {
		// Follower accepted the AppendEntries (heartbeat or with entries).
		// Update matchIndex and nextIndex to reflect confirmed replication.
		newMatch := prevLogIndex + len(entries)
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
			n.logfLocked("♥ heartbeat ack from Node %d — matchIndex=%d nextIndex=%d",
				peerID, n.matchIndex[peerID], n.nextIndex[peerID])
		}

		// Attempt commit advancement.
		n.advanceCommitIndex()
		shouldApply := n.lastApplied < n.commitIndex
		n.mu.Unlock()

		if shouldApply {
			n.applyCommitted()
		}
		return
	}

	// reply.Success == false: log inconsistency. Decrement nextIndex for retry.
	// The aggressive replicateToFollower loop handles this for SubmitCommand paths;
	// the heartbeat path does one step per tick (50ms between retries).
	if reply.ConflictIndex > 0 {
		n.nextIndex[peerID] = reply.ConflictIndex
	} else {
		n.nextIndex[peerID] = maxInt(1, n.nextIndex[peerID]-1)
	}
	n.logfLocked("♥ heartbeat rejected by Node %d — nextIndex→%d (conflictTerm=%d, conflictIndex=%d)",
		peerID, n.nextIndex[peerID], reply.ConflictTerm, reply.ConflictIndex)

	n.mu.Unlock()
}
