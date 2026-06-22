// node.go — MiniRaft Core Node Infrastructure
//
// This file defines the fundamental data structures and state management for a
// single Raft node. Everything else in the system (elections, heartbeats, log
// replication) builds on top of what is defined here.
//
// Architecture note:
//   A Raft node is fundamentally a replicated state machine driven by a
//   replicated log. The job of the consensus algorithm is to keep that log
//   identical across all nodes in the cluster, even in the presence of failures.
//
// Concurrency model:
//   All mutable state on a Node is protected by a single sync.RWMutex (mu).
//
//   Read operations (e.g., checking the term in an RPC handler) acquire a
//   read-lock (mu.RLock / mu.RUnlock). Multiple goroutines may hold read-locks
//   simultaneously.
//
//   Write operations (e.g., state transitions, log appends, term bumps) acquire
//   the exclusive write-lock (mu.Lock / mu.Unlock). Only one goroutine may hold
//   the write-lock at a time; all readers are blocked while it is held.
//
//   ⚠️  CRITICAL RULE — The lock MUST be released before making any RPC call.
//   RPCs involve network I/O and can block for arbitrary durations. Holding
//   the mutex across an RPC will deadlock the node when any other goroutine
//   (e.g., the RPC server handling an incoming call) attempts to acquire it.
//   This single rule is the source of the majority of bugs in Raft
//   implementations. It is enforced consistently throughout this codebase.
//
// Raft paper reference: §5 (https://raft.github.io/raft.pdf)

package main

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// NodeState — the role a node currently plays in the cluster
// ---------------------------------------------------------------------------

// NodeState represents the three mutually exclusive roles defined by the Raft
// protocol. At any point in time a node is exactly one of these.
type NodeState int

const (
	// Follower is the default, passive role. A follower:
	//   - Responds to RPCs from leaders and candidates.
	//   - If it receives no communication from a leader within the election
	//     timeout window, it transitions to Candidate and starts an election.
	Follower NodeState = iota // 0

	// Candidate is the intermediate role during an election. A candidate:
	//   - Has incremented its term and voted for itself.
	//   - Is actively soliciting votes from peers via RequestVote RPCs.
	//   - Becomes Leader on receiving a majority of votes.
	//   - Returns to Follower if it sees a higher term or a valid leader.
	Candidate // 1

	// Leader is the authoritative role. A leader:
	//   - Handles all client requests (command ingestion).
	//   - Replicates log entries to followers via AppendEntries RPCs.
	//   - Sends periodic heartbeats to prevent followers from timing out.
	//   - Tracks per-follower replication progress (nextIndex, matchIndex).
	Leader // 2
)

// String makes NodeState printable for structured logging.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// ---------------------------------------------------------------------------
// Timing constants
// ---------------------------------------------------------------------------

const (
	// HeartbeatInterval is how often the leader sends AppendEntries heartbeats.
	// This must be significantly shorter than the minimum election timeout so
	// that followers receive several heartbeats per timeout window and don't
	// trigger spurious elections on transient network hiccups.
	HeartbeatInterval = 50 * time.Millisecond

	// ElectionTimeoutMin and ElectionTimeoutMax define the randomized range for
	// the election timer. Each node picks a random duration in this range.
	//
	// Why randomized?
	//   If all nodes used the same timeout, they would all time out at the same
	//   moment after a leader crash and simultaneously start campaigns, leading
	//   to split votes where no candidate achieves majority. Randomization makes
	//   it probable that one node times out first, wins the election, and begins
	//   sending heartbeats before others even start campaigning.
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
)

// ---------------------------------------------------------------------------
// LogEntry — a single entry in the replicated log
// ---------------------------------------------------------------------------

// LogEntry represents one command in the replicated log. The Raft paper (§5.3)
// requires that each entry carry both an index and the term in which it was
// created. Together, (Index, Term) uniquely identifies an entry and is used to
// enforce the Log Matching Property.
//
// Log Matching Property (§5.3):
//
//	"If two logs contain an entry with the same index and term, then the logs
//	 are identical in all entries up through the given index."
//
// This property is guaranteed by the prevLogIndex / prevLogTerm consistency
// check performed in the AppendEntries RPC handler (see rpc.go).
type LogEntry struct {
	// Index is the 1-based position of this entry in the log.
	// Log indices start at 1. Index 0 is a sentinel / empty value.
	Index int

	// Term is the leader's term when this entry was created.
	// Used to detect stale entries and enforce the Log Matching Property.
	Term int

	// Command is the client-provided string to be executed on the state machine.
	// In a real system (e.g., etcd) this would be a serialized state machine
	// operation. In MiniRaft it is a human-readable string (e.g., "SET x=1").
	Command string
}

// ---------------------------------------------------------------------------
// Node — the central data structure
// ---------------------------------------------------------------------------

// Node is a single participant in the MiniRaft cluster. It encapsulates:
//   - Its identity within the cluster (id, peers).
//   - The three categories of state defined by the Raft paper.
//   - The RPC infrastructure for cluster communication.
//   - The timers that drive the Raft protocol.
//
// State categories per the Raft paper (Figure 2):
//
//	Persistent State  — Must survive crashes in a real implementation.
//	                    In MiniRaft we keep it in memory for simplicity,
//	                    but it is logically treated as persistent.
//
//	Volatile State    — Held by all nodes; lost on crash.
//
//	Leader State      — Held only while a node is leader; re-initialized
//	                    on every new election win.
type Node struct {
	// -----------------------------------------------------------------------
	// Identity (immutable after construction — no lock required)
	// -----------------------------------------------------------------------

	// id is this node's unique integer identifier within the cluster (1, 2, 3).
	id int

	// peers is a map of peerID → "host:port" address strings for every other
	// node in the cluster. Used to establish RPC client connections.
	peers map[int]string

	// -----------------------------------------------------------------------
	// Mutex — protects all mutable state below
	// -----------------------------------------------------------------------

	// mu is the single lock that serializes all state mutations on this node.
	// See the concurrency model notes at the top of this file.
	//
	// Pattern used throughout this codebase:
	//   n.mu.Lock()
	//   defer n.mu.Unlock()
	//   ... operate on state ...
	//
	// For read-heavy code paths (e.g., checking whether to vote):
	//   n.mu.RLock()
	//   defer n.mu.RUnlock()
	//   ... read state ...
	mu sync.RWMutex

	// -----------------------------------------------------------------------
	// Persistent State (logically persistent; in-memory in MiniRaft)
	// -----------------------------------------------------------------------

	// currentTerm is the latest term this node has observed. Terms are
	// monotonically increasing integers that act as a logical clock.
	//
	// Rule: If a node receives any RPC with term T > currentTerm, it must
	// immediately set currentTerm = T and revert to Follower. This rule
	// ensures that stale leaders cannot cause damage after a new term begins.
	currentTerm int

	// votedFor records which candidate this node voted for in the current term.
	// -1 means "has not voted". Reset to -1 whenever currentTerm advances.
	//
	// Rule: A node may grant at most one vote per term. Granting votes to
	// multiple candidates in the same term would allow two leaders to be
	// simultaneously elected (split brain).
	votedFor int

	// log is the sequence of LogEntry values this node has accepted.
	// Index 0 is a dummy entry to make 1-based indexing natural:
	//   log[0]  — sentinel (Index=0, Term=0, Command="")
	//   log[1]  — first real entry
	//   log[N]  — Nth real entry
	//
	// The dummy entry simplifies boundary conditions: prevLogIndex=0 and
	// prevLogTerm=0 are always valid and match on every node, allowing the
	// very first AppendEntries to succeed without special-casing.
	log []LogEntry

	// -----------------------------------------------------------------------
	// Volatile State — all nodes
	// -----------------------------------------------------------------------

	// commitIndex is the index of the highest log entry known to be committed.
	// "Committed" means the entry has been durably stored on a majority of
	// nodes and will never be removed. Initially 0.
	//
	// The leader advances commitIndex after receiving majority acknowledgements.
	// Followers learn the new commitIndex via the leaderCommit field of
	// AppendEntries (see rpc.go).
	commitIndex int

	// lastApplied is the index of the highest log entry applied to the state
	// machine. Always: lastApplied ≤ commitIndex.
	//
	// The gap [lastApplied+1 … commitIndex] is the queue of committed entries
	// waiting to be applied. In MiniRaft, "applying" logs a message. In a real
	// system it would execute the command against the storage engine.
	lastApplied int

	// -----------------------------------------------------------------------
	// Volatile State — leader only (re-initialized on each election win)
	// -----------------------------------------------------------------------

	// nextIndex[peerID] is the index of the next log entry to send to that
	// peer. Initialized to leader's lastLogIndex + 1 when elected.
	//
	// If a follower rejects AppendEntries (log inconsistency), the leader
	// decrements nextIndex[peerID] and retries. This is the log repair
	// mechanism described in §5.3 of the Raft paper.
	nextIndex map[int]int

	// matchIndex[peerID] is the index of the highest log entry known to be
	// replicated on that peer. Initialized to 0. Updated on successful
	// AppendEntries replies.
	//
	// The leader uses matchIndex to calculate the new commitIndex:
	//   commitIndex = largest N such that:
	//     - N > commitIndex
	//     - log[N].Term == currentTerm
	//     - matchIndex[i] >= N for a majority of peers i
	matchIndex map[int]int

	// -----------------------------------------------------------------------
	// Runtime — RPC infrastructure and timers
	// -----------------------------------------------------------------------

	// state is the current role of this node. Protected by mu.
	state NodeState

	// rpcClients holds one *rpc.Client per peer. Connections are established
	// at startup. A nil entry means the peer is currently unreachable and
	// a reconnection attempt will be made before the next RPC.
	rpcClients map[int]*rpc.Client

	// electionTimer fires when the node has not heard from a leader within
	// the randomized election timeout window. On fire, a Follower becomes
	// a Candidate and starts an election (see election.go).
	electionTimer *time.Timer

	// heartbeatTimer fires every HeartbeatInterval while this node is Leader.
	// On fire, the leader sends AppendEntries to all peers (see heartbeat.go).
	heartbeatTimer *time.Timer

	// -----------------------------------------------------------------------
	// Shutdown
	// -----------------------------------------------------------------------

	// dead is an atomic flag. 1 means this node has been "crashed" (stopped).
	// Using atomic int32 rather than a mutex-protected bool allows goroutines
	// to check the flag cheaply without acquiring mu, and avoids a deadlock
	// in the rare case where a goroutine checks dead while another holds mu
	// during a controlled shutdown sequence.
	dead int32

	// stateMachine is the underlying state machine that applies committed log entries.
	stateMachine *KVStore

	// metrics tracks operational visibility statistics (lock-free)
	metrics *Metrics
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

// NewNode creates and initializes a Raft node.
//
// Parameters:
//
//	id    — this node's unique integer ID (1-based)
//	peers — map of peerID → "host:port" for every OTHER node in the cluster
//
// The node starts as a Follower with term 0 and an empty log (containing only
// the sentinel entry at index 0). It does NOT start any goroutines or RPC
// servers here — that is the responsibility of the cluster bootstrap in main.go.
// This separation makes the node easy to unit-test in isolation.
func NewNode(id int, peers map[int]string) *Node {
	n := &Node{
		// Identity
		id:    id,
		peers: peers,

		// Persistent state — initial values from §5.1 of the Raft paper.
		currentTerm: 0,
		votedFor:    -1, // -1 means "not voted this term"

		// The log begins with a sentinel entry. This entry is never removed
		// or overwritten. It allows prevLogIndex=0 and prevLogTerm=0 to be
		// used as valid starting conditions in AppendEntries, eliminating
		// special-case logic for the very first log entry.
		log: []LogEntry{
			{Index: 0, Term: 0, Command: ""},
		},

		// Volatile state — both 0 at startup.
		commitIndex: 0,
		lastApplied: 0,

		// Leader state — allocated but empty; populated in becomeLeader().
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),

		// Runtime
		state:      Follower,
		rpcClients: make(map[int]*rpc.Client),

		// dead = 0 (alive)
		dead: 0,

		stateMachine: NewKVStore(),
		metrics:      &Metrics{},
	}

	n.restore()

	n.logf("initialized as %s (term=%d)", n.state, n.currentTerm)
	return n
}

// ---------------------------------------------------------------------------
// Lifecycle helpers
// ---------------------------------------------------------------------------

// Kill marks the node as crashed. All background goroutines check isDead()
// at the top of their loops and exit cleanly when it returns true.
//
// This is used in tests to simulate a node crash without actually killing the
// OS process. The node's in-memory state is preserved so tests can inspect it
// after a "crash".
func (n *Node) Kill() {
	atomic.StoreInt32(&n.dead, 1)
	n.logf("KILLED — simulating node crash")
}

// isDead returns true if Kill() has been called on this node.
// Used as the loop-exit condition in all background goroutines.
func (n *Node) isDead() bool {
	return atomic.LoadInt32(&n.dead) == 1
}

// Revive brings a killed node back to life, resetting it to Follower state.
// Used in failure_test.go and failure.go to simulate a crashed node rejoining.
//
// Note: RPC client connections to peers are cleared because TCP connections
// do not survive a "crash" — the reconnection logic in getClient() will
// re-establish them lazily on the next RPC attempt.
func (n *Node) Revive() {
	atomic.StoreInt32(&n.dead, 0)

	n.mu.Lock()

	n.state = Follower
	// Clear stale RPC clients — they point to closed connections.
	for id := range n.rpcClients {
		if n.rpcClients[id] != nil {
			n.rpcClients[id].Close()
			n.rpcClients[id] = nil
		}
	}
	// Clear leader state — it will be re-initialized when/if elected.
	n.nextIndex = make(map[int]int)
	n.matchIndex = make(map[int]int)

	// Nil out the election timer so StartElectionTicker creates a fresh one.
	// The old timer's goroutine (runElectionTicker) has already exited via
	// isDead() check, but its channel may still have a pending value.
	// A fresh timer ensures the new runElectionTicker goroutine reads from
	// a clean channel with no stale events.
	if n.electionTimer != nil {
		n.electionTimer.Stop()
		// Drain any pending event — non-blocking.
		select {
		case <-n.electionTimer.C:
		default:
		}
		n.electionTimer = nil
	}

	n.logfLocked("REVIVED — rejoining cluster as FOLLOWER (term=%d)", n.currentTerm)
	n.mu.Unlock()

	n.restoreSnapshot()
	n.restore()
}

// ---------------------------------------------------------------------------
// State transition helpers
// ---------------------------------------------------------------------------
// All state transitions are performed under n.mu.Lock(). These helpers
// enforce the legal transition graph:
//
//   Follower  → Candidate  (election timeout)
//   Candidate → Leader     (majority vote)
//   Candidate → Follower   (higher term seen / valid leader seen)
//   Leader    → Follower   (higher term seen)
//
// No other transitions are legal. Calling a transition that skips a step
// (e.g., Follower → Leader directly) is a protocol violation.

// becomeFollower transitions this node to the Follower role and updates its
// term. It is called when:
//   - A higher term is observed in any incoming or outgoing RPC.
//   - A valid AppendEntries is received while in Candidate state (another node
//     won the election).
//
// Callers MUST hold n.mu (write lock) before calling this method.
// The electionTimer is reset by the caller after releasing the lock to avoid
// holding the lock during timer operations.
func (n *Node) becomeFollower(term int) {
	prev := n.state
	if prev == Candidate {
		n.metrics.ElectionsLost.Add(1)
	}

	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1 // New term — clear the vote

	n.logfLocked("%s -> FOLLOWER (term=%d)", prev, term)
}

// becomeCandidate transitions this node to the Candidate role. It is called
// when the election timeout fires while in Follower or Candidate state.
//
// Actions:
//  1. Increment term (start a new election epoch).
//  2. Vote for self (every candidate votes for itself immediately).
//  3. Transition state to Candidate.
//
// Callers MUST hold n.mu (write lock) before calling this method.
func (n *Node) becomeCandidate() {
	if n.state == Candidate {
		// We timed out while already a Candidate. This means the previous
		// election was a split vote or no quorum was reached — we lost it.
		n.metrics.ElectionsLost.Add(1)
	}

	n.currentTerm++   // Begin a new term
	n.votedFor = n.id // Vote for self — always the first vote
	n.state = Candidate

	n.logfLocked("FOLLOWER -> CANDIDATE (term=%d)", n.currentTerm)
}

// becomeLeader transitions this node to the Leader role after winning an
// election. It is called when the candidate has accumulated majority votes.
//
// Actions:
//  1. Transition state to Leader.
//  2. Initialize nextIndex for each peer to lastLogIndex + 1.
//     (Optimistic assumption: peer has all entries the leader has.)
//  3. Initialize matchIndex for each peer to 0.
//     (Conservative assumption: we don't know what the peer has yet.)
//
// Callers MUST hold n.mu (write lock) before calling this method.
func (n *Node) becomeLeader() {
	n.state = Leader

	lastIdx := n.lastLogIndex()

	// Initialize per-follower tracking state. This is re-initialized on every
	// election win — stale values from a previous leadership term must not
	// carry over, as the cluster may have progressed while this node was down.
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIdx + 1
		n.matchIndex[peerID] = 0
	}

	n.metrics.ElectionsWon.Add(1)

	n.logfLocked("CANDIDATE -> LEADER (term=%d, lastLogIndex=%d)", n.currentTerm, lastIdx)
}

// ---------------------------------------------------------------------------
// Log helper methods
// ---------------------------------------------------------------------------

// lastLogIndex returns the index of the last entry in the log.
// Returns 0 if only the sentinel entry exists and it's index 0.
// If a snapshot exists, the sentinel entry might have a non-zero index.
//
// Callers should hold at least n.mu.RLock() before calling.
func (n *Node) lastLogIndex() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// LastLogIndex safely returns the index of the last entry in the log.
// Exported for concurrent use by external callers (like tests).
func (n *Node) LastLogIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogIndex()
}

// lastLogTerm returns the term of the last entry in the log.
// Returns 0 if only the sentinel entry exists and its term is 0.
//
// Callers should hold at least n.mu.RLock() before calling.
func (n *Node) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// getLogTerm returns the term of the log entry at the given index.
// Returns 0 for indices that don't exist in the log (safe default).
//
// Callers should hold at least n.mu.RLock() before calling.
func (n *Node) getLogTerm(index int) int {
	if len(n.log) == 0 {
		return 0
	}
	offset := n.log[0].Index
	if index < offset || index >= offset+len(n.log) {
		return 0
	}
	return n.log[index-offset].Term
}

// isLogUpToDate returns true if the candidate's log is at least as up-to-date
// as this node's log. This check is used during voting (see election.go).
//
// The Raft paper (§5.4.1) defines "up-to-date" as:
//   - The candidate's last log term is greater than this node's last log term, OR
//   - The terms are equal AND the candidate's last log index >= this node's.
//
// This ensures that a newly elected leader has all committed entries.
// A node that voted for a candidate guarantees the candidate's log is
// at least as complete as its own — and since committed entries must have
// been replicated to a majority, the new leader will always have them.
//
// Callers should hold at least n.mu.RLock() before calling.
func (n *Node) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
	myLastTerm := n.lastLogTerm()
	myLastIndex := n.lastLogIndex()

	if candidateLastLogTerm != myLastTerm {
		return candidateLastLogTerm > myLastTerm
	}
	return candidateLastLogIndex >= myLastIndex
}

// applyCommitted applies all log entries in the range (lastApplied, commitIndex]
// to the state machine. In MiniRaft the "state machine" simply logs the
// applied command. In a production system this would execute the command
// against the underlying storage engine (e.g., a B-tree or LSM-tree).
//
// This method is safe to call from any goroutine. It acquires and releases
// the lock internally. The lock is released before performing the "apply"
// action to avoid holding it during potentially slow I/O.
//
// Note: We log the applied entries without holding the lock. This is correct
// because we read from a local copy of the entries slice.
func (n *Node) applyCommitted() {
	n.mu.Lock()
	
	// Collect entries to apply without holding the lock for I/O.
	var toApply []LogEntry
	offset := n.log[0].Index
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		toApply = append(toApply, n.log[i-offset])
	}
	
	// Prevent other goroutines from applying the same entries
	if len(toApply) > 0 {
		n.lastApplied = n.commitIndex
	}
	n.mu.Unlock()

	// Apply each entry outside the lock.
	for _, entry := range toApply {
		n.metrics.CommandsApplied.Add(1)
		if err := n.stateMachine.Apply(entry.Command); err == nil {
			n.logf("APPLY  log[%d] term=%d cmd=%q", entry.Index, entry.Term, entry.Command)
		} else {
			n.logf("APPLY ERROR log[%d]: %v", entry.Index, err)
		}
	}

	n.mu.Lock()
	n.checkSnapshotThreshold()
	n.mu.Unlock()
}

// ---------------------------------------------------------------------------
// RPC client management
// ---------------------------------------------------------------------------

// getClient returns a live *rpc.Client for the given peer, (re-)connecting if
// necessary. Returns nil if the peer is currently unreachable.
//
// Connection lifecycle:
//   - Connections are established lazily on first use.
//   - If an existing connection is broken (detected when an RPC fails),
//     the caller sets n.rpcClients[peerID] = nil and retries.
//   - On the next call to getClient, a fresh connection is established.
//
// Callers MUST NOT hold n.mu when calling this method, because DialHTTP is a
// blocking network operation and would deadlock the node.
func (n *Node) getClient(peerID int) *rpc.Client {
	// Fast path — return existing live connection.
	n.mu.RLock()
	client := n.rpcClients[peerID]
	n.mu.RUnlock()

	if client != nil {
		return client
	}

	// Slow path — establish a new connection.
	addr, ok := n.peers[peerID]
	if !ok {
		n.logf("ERROR: unknown peer ID %d", peerID)
		return nil
	}

	// rpc.Dial uses TCP under the hood. We retry once on failure; persistent
	// failures are handled gracefully by returning nil (the caller skips the
	// peer for this round and retries next heartbeat/election cycle).
	newClient, err := rpc.Dial("tcp", addr)
	if err != nil {
		// This is expected during startup (peers may not be ready yet) and
		// after a peer crash. Log at debug level, not error level.
		n.logf("WARN: could not connect to peer %d (%s): %v", peerID, addr, err)
		return nil
	}

	n.mu.Lock()
	// Re-check under write lock — another goroutine may have connected first.
	if n.rpcClients[peerID] == nil {
		n.rpcClients[peerID] = newClient
	} else {
		// Another goroutine won the race; discard our connection.
		newClient.Close()
	}
	client = n.rpcClients[peerID]
	n.mu.Unlock()

	n.logf("connected to peer %d (%s)", peerID, addr)
	return client
}

// closeClient closes and nils the RPC client for peerID. Called when an RPC
// returns an error indicating the connection is broken.
//
// Callers MUST NOT hold n.mu when calling this method.
func (n *Node) closeClient(peerID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if c := n.rpcClients[peerID]; c != nil {
		c.Close()
		n.rpcClients[peerID] = nil
	}
}

// ---------------------------------------------------------------------------
// Term management
// ---------------------------------------------------------------------------

// checkTerm checks whether the incoming term is higher than this node's
// current term. If so, it steps down to Follower and updates the term.
//
// Returns true if the node's term was updated (i.e., a step-down occurred).
//
// This implements the universal Raft rule:
//
//	"If RPC request or response contains term T > currentTerm:
//	 set currentTerm = T, convert to follower." (§5.1)
//
// Callers MUST hold n.mu (write lock) before calling this method.
func (n *Node) checkTerm(incomingTerm int) bool {
	if incomingTerm > n.currentTerm {
		n.logfLocked("saw higher term %d (was %d) — stepping down", incomingTerm, n.currentTerm)
		n.becomeFollower(incomingTerm)
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Timer management
// ---------------------------------------------------------------------------

// resetElectionTimer resets the election timer to a new randomized duration.
// It reuses the SAME *time.Timer object (and thus the SAME channel) that
// runElectionTicker is blocking on. This is critical: creating a new timer
// would abandon the old channel, leaving runElectionTicker permanently blocked.
//
// Correct Go timer reset pattern:
//  1. Stop() — prevents the timer from firing during reset.
//  2. Non-blocking drain — if Stop() returns false, the value was already
//     sent to C; drain it so the next Reset() doesn't fire spuriously.
//  3. Reset(newDuration) — arms the timer with the new duration.
//
// Callers MUST NOT hold n.mu when calling this.
func (n *Node) resetElectionTimer() {
	if n.electionTimer == nil {
		// First call (from StartElectionTicker): create the timer.
		n.electionTimer = time.NewTimer(randomElectionTimeout())
		return
	}
	// Subsequent calls: reset the same timer object so runElectionTicker
	// keeps reading from the same channel (n.electionTimer.C).
	if !n.electionTimer.Stop() {
		// Timer already fired — drain the channel non-blocking to avoid
		// a spurious election trigger after Reset().
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(randomElectionTimeout())
}

// stopHeartbeatTimer stops the heartbeat timer if it is running.
// Called when a leader steps down to follower.
func (n *Node) stopHeartbeatTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}

// ---------------------------------------------------------------------------
// Structured logging
// ---------------------------------------------------------------------------

// logf emits a structured log line with the node's ID, current state, and
// current term. The format mirrors the debugging requirement from the spec:
//
//	[Node 2][Term 5][CANDIDATE] message here
//
// The node's state and term are read under a read-lock. The log.Printf call
// itself happens outside the lock to avoid holding it during I/O.
func (n *Node) logf(format string, args ...interface{}) {
	n.mu.RLock()
	id := n.id
	state := n.state
	term := n.currentTerm
	n.mu.RUnlock()

	prefix := fmt.Sprintf("[Node %d][Term %d][%s] ", id, term, state)
	log.Printf(prefix+format, args...)
}

// logfLocked is like logf but must be called while already holding n.mu
// (either read or write lock). It does NOT acquire the lock itself.
// Use this when you need to log inside a critical section.
func (n *Node) logfLocked(format string, args ...interface{}) {
	prefix := fmt.Sprintf("[Node %d][Term %d][%s] ", n.id, n.currentTerm, n.state)
	log.Printf(prefix+format, args...)
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// quorumSize returns the minimum number of nodes (including self) required
// to form a majority in this cluster. For 3 nodes: quorumSize = 2.
//
// Formula: ⌊(N+1)/2⌋ + 1 for N total nodes... simplified to (N/2 + 1)
// where N = len(peers) + 1 (peers does not include self).
func (n *Node) quorumSize() int {
	total := len(n.peers) + 1 // +1 for self
	return total/2 + 1
}

// GetState returns a snapshot of the node's current term and whether it
// believes it is the leader. Exported for use in tests.
func (n *Node) GetState() (term int, isLeader bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm, n.state == Leader
}

// GetCommitIndex returns the node's current commitIndex. Exported for tests.
func (n *Node) GetCommitIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

// GetLog returns a copy of the node's log (excluding the sentinel entry at
// index 0). Exported for tests to verify replication correctness.
// updating the get log
func (n *Node) GetLog() []LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Return a complete copy including the sentinel entry.
	// This preserves Raft log indexing:
	// log[0] = sentinel
	// log[1] = first real command
	result := make([]LogEntry, len(n.log))
	copy(result, n.log)

	return result
}

// GetStateMachine returns the node's KVStore state machine.
// Exported for tests.
func (n *Node) GetStateMachine() *KVStore {
	return n.stateMachine
}
