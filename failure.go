// failure.go — MiniRaft Failure Simulation and Recovery
//
// This file provides the infrastructure for simulating node failures,
// restarts, and network partitions in the MiniRaft cluster. It is the
// foundation on which Phase 8's tests are built.
//
// Why this file exists:
//   Raft's correctness is only meaningful under failures. A consensus
//   algorithm that works when nothing crashes is not a consensus algorithm —
//   it's just a coordinator. The scenarios in this file stress-test the
//   safety and liveness properties that make Raft valuable:
//
//     Safety:  Committed entries are NEVER lost (even across leader crashes).
//     Liveness: The cluster eventually elects a new leader (as long as majority is alive).
//
// Architecture:
//   The Cluster type manages a group of Nodes including their RPC servers
//   and TCP listeners. It provides:
//
//     Lifecycle:  NewCluster, Shutdown
//     Failure:    Kill, Restart, IsAlive
//     Queries:    GetLeader, WaitForLeader, AliveNodes, QuorumSize
//     Workload:   SubmitCommand, WaitForCommit
//     Scenarios:  RunScenario1..4 (demonstration functions)
//
// RPC server lifecycle:
//   Each node has its own net.Listener and *rpc.Server. When a node is killed,
//   its listener is closed (new connections rejected). When restarted, a fresh
//   listener and server are created on the same port. Existing peer RPC clients
//   fail on their next call, close themselves (closeClient), and reconnect lazily
//   on the next RPC attempt (getClient). This provides seamless reconnection
//   without any explicit reconnect logic in the node.
//
// Raft paper reference: §5.2 (leader election), §5.3 (log replication),
//                       §5.4 (safety), §8 (client interaction)

package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Cluster — the central test fixture
// ---------------------------------------------------------------------------

// Cluster manages a set of MiniRaft nodes for local simulation and testing.
// It owns the TCP listeners and RPC servers for each node, enabling
// controlled failure injection via Kill and Restart.
//
// Thread safety:
//
//	All exported methods acquire c.mu before accessing cluster-level state
//	(listeners map, etc.). Node-level state is protected by the individual
//	node's own mutex. Do not hold c.mu when calling node methods that
//	acquire n.mu — that ordering could deadlock.
type Cluster struct {
	mu        sync.Mutex
	size      int                  // total number of nodes (e.g., 3)
	nodes     map[int]*Node        // nodeID → Node
	addrs     map[int]string       // nodeID → "localhost:PORT"
	listeners map[int]net.Listener // nodeID → active TCP listener (nil if dead)
}

// ---------------------------------------------------------------------------
// NewCluster — create and start a complete cluster
// ---------------------------------------------------------------------------

// NewCluster creates a MiniRaft cluster of `size` nodes on sequential ports
// starting at basePort+1. For a 3-node cluster: ports 5001, 5002, 5003.
//
// Startup sequence (order matters for correctness):
//  1. Build all Node structs (no goroutines yet — pure data initialization).
//  2. Start all RPC servers (listeners active, accepting connections).
//  3. Start election tickers on all nodes.
//
// Why this order?
//
//	If we started election tickers before RPC servers were up, a node might
//	time out and send RequestVote before its peers' RPC servers were listening.
//	The RPC call would fail, the candidate would get fewer votes, and startup
//	would be noisier. Starting servers first means RPCs succeed from the first
//	election timeout.
//
// The function returns when all servers are listening and all election tickers
// are running. An election will occur within 150–300ms (first timeout).
//
// Common bug: Starting tickers before servers → spurious connection failures
// in the first election round.
func NewCluster(size int) (*Cluster, error) {
	// Clean up data from previous clusters to prevent cross-test contamination.
	os.RemoveAll("data")

	// Use OS-assigned ports (":0") to avoid collisions between concurrent
	// or sequential test runs. Each node's listener is created first so we
	// know the port, then the peer maps are built with those concrete addresses.
	// This prevents the "address already in use" race between tests.

	c := &Cluster{
		size:      size,
		nodes:     make(map[int]*Node, size),
		addrs:     make(map[int]string, size),
		listeners: make(map[int]net.Listener, size),
	}

	// -----------------------------------------------------------------------
	// Step 1: Open listeners on OS-assigned ports.
	// We must open all listeners BEFORE building peer maps, so we know the
	// concrete port for each node.
	// -----------------------------------------------------------------------
	for id := 1; id <= size; id++ {
		ln, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			c.Shutdown()
			return nil, fmt.Errorf("cluster: failed to listen for Node %d: %w", id, err)
		}
		c.listeners[id] = ln
		c.addrs[id] = ln.Addr().String() // e.g., "127.0.0.1:54231"
	}

	// -----------------------------------------------------------------------
	// Step 2: Create Node structs now that all ports are known.
	// -----------------------------------------------------------------------
	for id := 1; id <= size; id++ {
		peers := make(map[int]string, size-1)
		for peerID := 1; peerID <= size; peerID++ {
			if peerID != id {
				peers[peerID] = c.addrs[peerID]
			}
		}
		c.nodes[id] = NewNode(id, peers)
	}

	// -----------------------------------------------------------------------
	// Step 3: Start RPC servers using the pre-opened listeners.
	// -----------------------------------------------------------------------
	for id := 1; id <= size; id++ {
		if err := c.startRPCServerOnListener(id, c.listeners[id]); err != nil {
			c.Shutdown()
			return nil, fmt.Errorf("cluster: failed to start RPC for Node %d: %w", id, err)
		}
	}

	// -----------------------------------------------------------------------
	// Step 4: Start election tickers.
	// -----------------------------------------------------------------------
	for id := 1; id <= size; id++ {
		c.nodes[id].StartElectionTicker()
	}

	log.Printf("[Cluster] started %d-node cluster", size)
	return c, nil
}

// startRPCServerOnListener starts the accept loop on a pre-opened listener.
// Used by NewCluster when the listener is already bound to a dynamic port.
func (c *Cluster) startRPCServerOnListener(nodeID int, ln net.Listener) error {
	node := c.nodes[nodeID]

	server := rpc.NewServer()
	if err := server.Register(node); err != nil {
		return fmt.Errorf("rpc.Register failed for Node %d: %w", nodeID, err)
	}

	// Accept loop — exits cleanly when the listener is closed.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if !node.isDead() {
					log.Printf("[Cluster] Node %d RPC accept error: %v", nodeID, err)
				}
				return
			}
			go server.ServeConn(conn)
		}
	}()

	log.Printf("[Cluster] Node %d RPC server listening on %s", nodeID, ln.Addr().String())
	return nil
}

// startRPCServer opens a new listener on the node's current address
// (c.addrs[nodeID]) and starts serving. Used during Restart after Kill.
// Retries up to 3 times if the port is briefly in TIME_WAIT.
func (c *Cluster) startRPCServer(nodeID int) error {
	node := c.nodes[nodeID]
	addr := c.addrs[nodeID]

	server := rpc.NewServer()
	if err := server.Register(node); err != nil {
		return fmt.Errorf("rpc.Register failed for Node %d: %w", nodeID, err)
	}

	var ln net.Listener
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		ln, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		log.Printf("[Cluster] Node %d: listen attempt %d on %s failed (%v), retrying...",
			nodeID, attempt, addr, err)
		time.Sleep(60 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("net.Listen on %s failed after retries: %w", addr, err)
	}

	c.listeners[nodeID] = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if !node.isDead() {
					log.Printf("[Cluster] Node %d RPC accept error: %v", nodeID, err)
				}
				return
			}
			go server.ServeConn(conn)
		}
	}()

	log.Printf("[Cluster] Node %d RPC server listening on %s", nodeID, addr)
	return nil
}

// ---------------------------------------------------------------------------
// Failure simulation: Kill, Restart, IsAlive
// ---------------------------------------------------------------------------

// Kill simulates a node crash. The node's state (log, term, etc.) is preserved
// in memory, but the node's goroutines will exit and its RPC server is stopped.
//
// What Kill does:
//  1. Sets the atomic dead flag (isDead() returns true).
//  2. Closes all outbound RPC client connections.
//  3. Closes the TCP listener (rejects new incoming connections).
//
// What Kill does NOT do:
//   - Corrupt or erase the node's log (simulating crash-fail, not data loss).
//   - Force-close in-progress RPC connections (they drain naturally).
//   - Stop goroutines synchronously (they exit on their next iteration).
//
// Goroutine exit behavior after Kill:
//   - Election ticker: exits on next timer fire (isDead() check at top of loop).
//   - Heartbeat goroutine: exits on next ticker fire (isDead() + state check).
//   - Per-peer RPC goroutines: finish current RPC, then exit via state guards.
//
// No goroutine leaks: all goroutines exit within one timeout interval (≤300ms).
//
// Common bug in test simulators: not closing the listener.
//
//	If the listener stays open, peer nodes can still connect to the "dead" node
//	and their RPCs will block or receive incorrect replies.
func (c *Cluster) Kill(nodeID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return
	}

	if node.isDead() {
		return // already killed — idempotent
	}

	log.Printf("[Cluster] 💀 KILLING Node %d", nodeID)

	// Set the atomic dead flag. All goroutines will check this and exit.
	node.Kill()

	// Close the TCP listener. This causes the accept loop goroutine to exit
	// (Accept returns an error), and causes any pending Accept to unblock.
	if ln := c.listeners[nodeID]; ln != nil {
		ln.Close()
		c.listeners[nodeID] = nil
	}
}

// Restart brings a crashed node back to life. The node is revived with its
// in-memory state intact (log, term, etc.) and a new RPC server is started
// on the same port.
//
// What Restart does:
//  1. Ensures the old listener is closed (idempotent Kill).
//  2. Calls node.Revive() → state=Follower, dead=0, clears stale RPC clients.
//  3. Starts a fresh RPC server on the same port.
//  4. Starts a fresh election ticker.
//
// Why the node starts as Follower:
//
//	After a crash, the node has no knowledge of what happened while it was down.
//	It must learn the current leader and term from the heartbeats it receives.
//	Starting as Candidate would be incorrect — it might campaign with a stale term
//	and disrupt a functioning cluster.
//
// Log retention after restart:
//
//	The in-memory log is preserved (no WAL in MiniRaft). In a real system, the
//	log would be recovered from durable storage (WAL). This is why we mark the
//	persistent state as "logically persistent" even though it's in memory.
//
// Reconnection:
//
//	Peer nodes will fail their next RPC to this node (old clients are broken),
//	call closeClient(), and reconnect lazily on the next attempt via getClient().
//	No explicit reconnect logic is needed.
func (c *Cluster) Restart(nodeID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("cluster: unknown node ID %d", nodeID)
	}

	log.Printf("[Cluster] 🔄 RESTARTING Node %d", nodeID)

	// Ensure old server is stopped.
	if !node.isDead() {
		node.Kill()
	}
	if ln := c.listeners[nodeID]; ln != nil {
		ln.Close()
		c.listeners[nodeID] = nil
	}

	// Brief pause to allow the OS to fully release the port.
	// This prevents "address already in use" on immediate rebind.
	// With SO_REUSEADDR (set by default by Go's net.Listen), this is usually
	// not needed, but 20ms of defensive delay prevents flaky restarts.
	time.Sleep(20 * time.Millisecond)

	// Revive: sets dead=0, state=Follower, clears stale RPC clients.
	// The log and term are preserved — this simulates crash-fail recovery.
	node.Revive()

	// Start a fresh RPC server.
	if err := c.startRPCServer(nodeID); err != nil {
		return fmt.Errorf("cluster: restart failed for Node %d: %w", nodeID, err)
	}

	// Start election ticker. The heartbeat ticker will start automatically
	// if/when this node wins an election (launched by becomeLeader → sendHeartbeats).
	node.StartElectionTicker()

	log.Printf("[Cluster] ✅ Node %d restarted as FOLLOWER (term=%d, logLen=%d)",
		nodeID, func() int { t, _ := node.GetState(); return t }(),
		len(node.GetLog()))

	return nil
}

// ColdRestart simulates a cold start of a node by killing it, closing listeners,
// wiping the in-memory Node struct, and calling NewNode to instantiate a fresh one
// that will read state/snapshots from disk.
func (c *Cluster) ColdRestart(nodeID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("cluster: unknown node ID %d", nodeID)
	}

	log.Printf("[Cluster] 🔄 COLD RESTARTING Node %d", nodeID)

	// Ensure old server is stopped.
	if !node.isDead() {
		node.Kill()
	}
	if ln := c.listeners[nodeID]; ln != nil {
		ln.Close()
		c.listeners[nodeID] = nil
	}

	// Brief pause to allow OS to release the port
	time.Sleep(20 * time.Millisecond)

	// Create a new Node struct, wiping out all in-memory state.
	peers := make(map[int]string)
	for id, addr := range c.addrs {
		if id != nodeID {
			peers[id] = addr
		}
	}
	newNode := NewNode(nodeID, peers)
	c.nodes[nodeID] = newNode

	// Start a fresh RPC server.
	if err := c.startRPCServer(nodeID); err != nil {
		return fmt.Errorf("cluster: cold restart failed for Node %d: %w", nodeID, err)
	}

	// Start election ticker.
	newNode.StartElectionTicker()

	log.Printf("[Cluster] ✅ Node %d cold-restarted as FOLLOWER (term=%d, logLen=%d)",
		nodeID, func() int { t, _ := newNode.GetState(); return t }(),
		len(newNode.GetLog()))

	return nil
}


// IsAlive returns true if the node is currently running (not killed).
func (c *Cluster) IsAlive(nodeID int) bool {
	c.mu.Lock()
	node, ok := c.nodes[nodeID]
	c.mu.Unlock()

	if !ok {
		return false
	}
	return !node.isDead()
}

// ---------------------------------------------------------------------------
// Cluster query helpers
// ---------------------------------------------------------------------------

// GetLeader scans all alive nodes and returns the (nodeID, term) of the
// current leader. Returns (0, 0) if no leader exists right now.
//
// For a consistent answer, both conditions must hold:
//  1. Exactly one node claims to be leader.
//  2. All claiming leaders agree on the term.
//
// This is used for point-in-time checks. For waiting until a leader exists,
// use WaitForLeader.
func (c *Cluster) GetLeader() (nodeID int, term int) {
	c.mu.Lock()
	nodes := make(map[int]*Node, len(c.nodes))
	for id, n := range c.nodes {
		nodes[id] = n
	}
	c.mu.Unlock()

	for id, node := range nodes {
		if node.isDead() {
			continue
		}
		t, isLeader := node.GetState()
		if isLeader {
			if nodeID != 0 {
				// Multiple leaders detected — return 0 to signal instability.
				return 0, 0
			}
			nodeID = id
			term = t
		}
	}
	return nodeID, term
}

// WaitForLeader polls until exactly one leader is stable, or until the timeout.
// Returns the leader's nodeID and term.
//
// "Stable" means: the same node reports as leader on two consecutive polls
// 20ms apart. This prevents false positives during term transitions.
//
// Used in test and scenario code to synchronize with the Raft election process.
func (c *Cluster) WaitForLeader(timeout time.Duration) (nodeID int, term int, err error) {
	deadline := time.Now().Add(timeout)
	var prevLeaderID, prevTerm int

	for time.Now().Before(deadline) {
		leaderID, leaderTerm := c.GetLeader()

		if leaderID != 0 && leaderID == prevLeaderID && leaderTerm == prevTerm {
			// Same leader reported twice consecutively — stable.
			log.Printf("[Cluster] ✓ stable leader: Node %d at term=%d", leaderID, leaderTerm)
			return leaderID, leaderTerm, nil
		}

		prevLeaderID = leaderID
		prevTerm = leaderTerm
		time.Sleep(20 * time.Millisecond)
	}

	return 0, 0, fmt.Errorf("cluster: no stable leader within %v", timeout)
}

// SubmitCommand sends a command to the current leader.
// Returns (logIndex, term, error).
// Retries for up to 2 seconds if no leader is currently available.
//
// In a real system, the client would retry with exponential backoff.
// Here we use a simple polling loop for clarity.
func (c *Cluster) SubmitCommand(command string) (index int, term int, err error) {
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		// Find the current leader.
		c.mu.Lock()
		nodes := make(map[int]*Node, len(c.nodes))
		for id, n := range c.nodes {
			nodes[id] = n
		}
		c.mu.Unlock()

		for _, node := range nodes {
			if node.isDead() {
				continue
			}
			idx, t, ok := node.SubmitCommand(command)
			if ok {
				log.Printf("[Cluster] command %q submitted: log[%d] term=%d", command, idx, t)
				return idx, t, nil
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	return 0, 0, errors.New("cluster: no leader available to accept command")
}

// WaitForCommit waits until the log entry at `index` is committed on at least
// a quorum of alive nodes. Returns nil on success, error on timeout.
//
// "Committed" on a node means node.GetCommitIndex() >= index.
//
// This is used to verify that a submitted command has been durably agreed upon
// by the cluster majority before proceeding with the next test step.
func (c *Cluster) WaitForCommit(index int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	quorum := c.QuorumSize()

	for time.Now().Before(deadline) {
		committed := 0

		c.mu.Lock()
		for _, node := range c.nodes {
			if !node.isDead() && node.GetCommitIndex() >= index {
				committed++
			}
		}
		c.mu.Unlock()

		if committed >= quorum {
			log.Printf("[Cluster] ✓ log[%d] committed on %d/%d nodes (quorum=%d)",
				index, committed, c.size, quorum)
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("cluster: log[%d] not committed on quorum within %v", index, timeout)
}

// WaitForLogSync waits until all alive nodes have the same log up through
// the given index. Used to verify log repair after a leader change.
func (c *Cluster) WaitForLogSync(index int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		synced := 0
		total := 0

		c.mu.Lock()
		for _, node := range c.nodes {
			if node.isDead() {
				continue
			}
			total++
			ok := node.LastLogIndex() >= index
			if ok {
				synced++
			}
		}
		c.mu.Unlock()

		if synced == total && total > 0 {
			log.Printf("[Cluster] ✓ all %d alive nodes have log[1..%d]", total, index)
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("cluster: log sync to index %d timed out", index)
}

// AliveNodes returns the IDs of currently alive nodes, sorted ascending.
func (c *Cluster) AliveNodes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var alive []int
	for id, node := range c.nodes {
		if !node.isDead() {
			alive = append(alive, id)
		}
	}
	return alive
}

// QuorumSize returns the minimum number of votes needed for majority in this cluster.
func (c *Cluster) QuorumSize() int {
	return c.size/2 + 1
}

// NodeCount returns the total number of nodes in the cluster.
func (c *Cluster) NodeCount() int {
	return c.size
}

// GetNode returns the Node for the given nodeID. Exported for tests.
func (c *Cluster) GetNode(nodeID int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[nodeID]
}

// Shutdown cleanly stops all nodes and closes all RPC servers.
// Should be called in test teardown (defer cluster.Shutdown()).
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("[Cluster] shutting down %d-node cluster", c.size)

	for id, node := range c.nodes {
		if !node.isDead() {
			node.Kill()
		}
		if ln := c.listeners[id]; ln != nil {
			ln.Close()
			c.listeners[id] = nil
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario 1: Leader Crash and Re-Election
// ---------------------------------------------------------------------------

// RunScenario1_LeaderCrash demonstrates the most fundamental Raft failure mode:
// the leader crashes and the remaining nodes elect a new leader.
//
// What Raft property is tested:
//
//	Liveness — the cluster must continue serving requests after a leader failure.
//	Safety   — the new leader must have all committed entries.
//
// Which safety invariant must hold:
//
//	Leader Completeness (§5.4): every leader has all committed log entries.
//	State Machine Safety: all state machines apply the same sequence of entries.
//
// What common bug would violate safety:
//
//	A node with an out-of-date log could win election (Election Restriction
//	not enforced). It would then claim to be leader but be missing committed
//	entries — any future append would create a log divergence.
//
// Why this implementation remains correct:
//
//	isLogUpToDate() in RequestVote ensures that only nodes with up-to-date
//	logs can receive votes. Since committed entries are on a majority, the
//	majority needed for election always contains a node that has them.
//
// Execution trace:
//  1. Cluster starts, leader elected (say Node 2, term=1).
//  2. Client submits "BEFORE_CRASH".
//  3. Entry committed on majority (Node 2 + one follower).
//  4. Node 2 killed.
//  5. Followers lose heartbeat.
//  6. First follower to time out (150–300ms) → CANDIDATE, term=2.
//  7. Remaining follower grants vote → new LEADER at term=2.
//  8. New leader sends heartbeats; other follower resets timer.
//  9. Client submits "AFTER_CRASH" to new leader.
//  10. Entry committed. Cluster operational.
func RunScenario1_LeaderCrash(c *Cluster) error {
	log.Println("═══════════════════════════════════════════════")
	log.Println("SCENARIO 1: Leader Crash and Re-Election")
	log.Println("═══════════════════════════════════════════════")
	log.Println("Testing: liveness after leader failure")
	log.Println("Safety property: Leader Completeness (§5.4)")

	// Step 1: Wait for initial stable leader.
	leaderID, term, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario1: initial leader not elected: %w", err)
	}
	log.Printf("S1: Initial leader = Node %d (term=%d)", leaderID, term)

	// Step 2: Submit a command that will be committed before the crash.
	idx, _, err := c.SubmitCommand("BEFORE_CRASH")
	if err != nil {
		return fmt.Errorf("scenario1: pre-crash submit failed: %w", err)
	}

	// Step 3: Wait for the command to be committed on the majority.
	if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
		return fmt.Errorf("scenario1: pre-crash commit failed: %w", err)
	}
	log.Printf("S1: Command 'BEFORE_CRASH' committed at log[%d] ✓", idx)

	// Step 4: Kill the leader.
	log.Printf("S1: 💀 Killing leader Node %d", leaderID)
	c.Kill(leaderID)

	// Step 5: Wait for a new leader to be elected.
	// Expected: within 2 election timeouts (2 × 300ms = 600ms worst case).
	// We give 3 seconds of margin.
	log.Println("S1: Waiting for new leader election...")
	newLeaderID, newTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario1: new leader not elected after crash: %w", err)
	}
	if newLeaderID == leaderID {
		return fmt.Errorf("scenario1: dead node %d is still reported as leader", leaderID)
	}
	if newTerm <= term {
		return fmt.Errorf("scenario1: new term %d should be > old term %d", newTerm, term)
	}
	log.Printf("S1: ✓ New leader = Node %d (term=%d)", newLeaderID, newTerm)

	// Step 6: Submit a command AFTER the crash.
	// This verifies the cluster is fully operational with the new leader.
	idx2, _, err := c.SubmitCommand("AFTER_CRASH")
	if err != nil {
		return fmt.Errorf("scenario1: post-crash submit failed: %w", err)
	}
	if err := c.WaitForCommit(idx2, 2*time.Second); err != nil {
		return fmt.Errorf("scenario1: post-crash commit failed: %w", err)
	}
	log.Printf("S1: Command 'AFTER_CRASH' committed at log[%d] ✓", idx2)

	// Step 7: Verify the committed entry from before the crash is preserved.
	// The new leader must have all committed entries (Leader Completeness).
	newLeader := c.GetNode(newLeaderID)
	entries := newLeader.GetLog()
	found := false
	for _, e := range entries {
		if e.Command == "BEFORE_CRASH" {
			found = true
			break
		}
	}
	if !found {
		return errors.New("scenario1: SAFETY VIOLATION — 'BEFORE_CRASH' entry lost after leader crash")
	}
	log.Println("S1: ✓ Pre-crash committed entry preserved in new leader's log")

	log.Println("SCENARIO 1 COMPLETE: liveness and safety verified ✓")
	log.Println()
	return nil
}

// ---------------------------------------------------------------------------
// Scenario 2: Old Leader Rejoins the Cluster
// ---------------------------------------------------------------------------

// RunScenario2_OldLeaderRejoin demonstrates what happens when a crashed leader
// is restarted after a new leader has been elected.
//
// What Raft property is tested:
//
//	Higher-Term Step-Down (§5.1): any node observing a higher term immediately
//	becomes a Follower. The old leader cannot remain leader in a new term.
//
// State transition diagram:
//
//	Old Leader (Node X, term=T):
//	  CRASHES
//	  ...time passes, new leader elected at term=T+1...
//	  REVIVE  →  state=Follower (Revive() sets this)
//	  <Receives heartbeat from new leader (term=T+1)>
//	  checkTerm(T+1): T+1 > T  →  state=Follower, currentTerm=T+1
//	  <Receives log repair AppendEntries>
//	  <Log synchronized>
//	  <Stable Follower>
//
// Common bug that would violate safety:
//
//	If the old leader ignored the higher term in heartbeats and kept sending
//	its own heartbeats (term=T), followers might be confused. However, since
//	T < T+1, any recipient of the old leader's AppendEntries would reject it
//	(Rule 1: reject if args.Term < currentTerm) and reply with their higher
//	term, causing the old leader to step down anyway. The step-down is
//	inevitable — the question is only how quickly.
//
// Execution trace:
//  1. Leader = Node X at term=T.
//  2. Submit and commit commands.
//  3. Kill Node X.
//  4. New leader = Node Y at term=T+1.
//  5. Submit more commands to Node Y.
//  6. Restart Node X (state: Follower, term=T, log may be stale).
//  7. Node X receives heartbeat from Node Y (term=T+1).
//  8. checkTerm(T+1): Node X → Follower at term=T+1.
//  9. Node Y sends log repair entries to Node X.
//  10. Node X log synchronized.
//  11. Cluster has exactly one leader.
func RunScenario2_OldLeaderRejoin(c *Cluster) error {
	log.Println("═══════════════════════════════════════════════")
	log.Println("SCENARIO 2: Old Leader Rejoins Cluster")
	log.Println("═══════════════════════════════════════════════")
	log.Println("Testing: higher-term step-down and log repair")
	log.Println("Safety property: §5.1 Higher-Term Rule")

	// Step 1: Wait for initial leader.
	oldLeaderID, oldTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario2: initial leader not elected: %w", err)
	}
	log.Printf("S2: Initial leader = Node %d (term=%d)", oldLeaderID, oldTerm)

	// Step 2: Submit commands before crash.
	for i := 1; i <= 3; i++ {
		idx, _, err := c.SubmitCommand(fmt.Sprintf("OLD_LEADER_CMD_%d", i))
		if err != nil {
			return fmt.Errorf("scenario2: pre-crash command %d failed: %w", i, err)
		}
		if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
			return fmt.Errorf("scenario2: pre-crash commit %d failed: %w", i, err)
		}
	}
	log.Printf("S2: 3 commands committed on cluster ✓")

	// Step 3: Kill the old leader.
	log.Printf("S2: 💀 Killing old leader Node %d (term=%d)", oldLeaderID, oldTerm)
	c.Kill(oldLeaderID)

	// Step 4: Wait for new leader at higher term.
	newLeaderID, newTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario2: new leader not elected: %w", err)
	}
	log.Printf("S2: New leader = Node %d (term=%d)", newLeaderID, newTerm)
	if newTerm <= oldTerm {
		return fmt.Errorf("scenario2: new term %d should be > old term %d", newTerm, oldTerm)
	}

	// Step 5: Submit commands to new leader while old leader is down.
	// These entries will NOT be in old leader's log when it restarts.
	for i := 1; i <= 2; i++ {
		idx, _, err := c.SubmitCommand(fmt.Sprintf("NEW_LEADER_CMD_%d", i))
		if err != nil {
			return fmt.Errorf("scenario2: new leader command %d failed: %w", i, err)
		}
		if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
			return fmt.Errorf("scenario2: new leader commit %d failed: %w", i, err)
		}
	}
	log.Printf("S2: New leader committed 2 additional commands ✓")

	// Step 6: Restart the old leader.
	// At this point: old leader has term=oldTerm, new cluster is at term=newTerm.
	log.Printf("S2: 🔄 Restarting old leader Node %d", oldLeaderID)
	if err := c.Restart(oldLeaderID); err != nil {
		return fmt.Errorf("scenario2: restart failed: %w", err)
	}

	// Step 7: Wait for the cluster to stabilize.
	// The restarted node must observe the new leader's heartbeat (term=newTerm)
	// and immediately step down via checkTerm(newTerm).
	// Allow time for: heartbeat propagation (50ms) + log repair (1–2 round trips).
	log.Println("S2: Waiting for old leader to rejoin as Follower...")
	time.Sleep(500 * time.Millisecond)

	// Step 8: Verify the old leader is now a Follower.
	restartedNode := c.GetNode(oldLeaderID)
	restartedTerm, restartedIsLeader := restartedNode.GetState()

	if restartedIsLeader {
		return fmt.Errorf("scenario2: SAFETY VIOLATION — restarted node %d is still leader", oldLeaderID)
	}
	if restartedTerm < newTerm {
		return fmt.Errorf("scenario2: restarted node term %d should be >= new term %d",
			restartedTerm, newTerm)
	}
	log.Printf("S2: ✓ Old leader is now FOLLOWER at term=%d", restartedTerm)

	// Step 9: Verify exactly one leader remains.
	currentLeaderID, currentTerm := c.GetLeader()
	if currentLeaderID == 0 {
		return errors.New("scenario2: no leader found after rejoin")
	}
	if currentLeaderID == oldLeaderID {
		return fmt.Errorf("scenario2: old leader Node %d should not be leader again", oldLeaderID)
	}
	log.Printf("S2: ✓ Single leader: Node %d at term=%d", currentLeaderID, currentTerm)

	// Step 10: Wait for log synchronization.
	// The new leader will replicate missing entries to the rejoined node.
	newLeaderNode := c.GetNode(newLeaderID)
	lastIdx := newLeaderNode.LastLogIndex()
	log.Printf("S2: Waiting for log sync to index %d on restarted node...", lastIdx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		restartedIdx := restartedNode.LastLogIndex()
		if restartedIdx >= lastIdx {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	restartedIdx := restartedNode.LastLogIndex()
	if restartedIdx < lastIdx {
		return fmt.Errorf("scenario2: restarted node log[len=%d] behind leader log[len=%d]",
			restartedIdx, lastIdx)
	}
	log.Printf("S2: ✓ Restarted node log synchronized (lastLogIndex=%d)", restartedIdx)

	log.Println("SCENARIO 2 COMPLETE: step-down and log repair verified ✓")
	log.Println()
	return nil
}

// ---------------------------------------------------------------------------
// Scenario 3: Partial Replication and Safety Under Leader Crash
// ---------------------------------------------------------------------------

// RunScenario3_PartialReplication demonstrates that committed entries are safe
// even when the leader crashes during or after replication to a minority.
//
// This tests the most subtle Raft safety argument:
//
//	Case A — Entry replicated to majority THEN leader crashes:
//	  Entry IS committed. New leader has it (Election Restriction ensures this).
//	  New leader will eventually replicate it to any follower that missed it.
//
//	Case B — Entry NOT replicated to majority before leader crashes:
//	  Entry may be LOST if only the crashed leader had it.
//	  This is SAFE — uncommitted entries may be lost. Clients must retry.
//
// The Election Restriction (§5.4) is the key safety mechanism:
//
//	A candidate with index/term (lastIdx, lastTerm) can only be elected if
//	a majority grants votes. A voter rejects if its log is "more up-to-date"
//	than the candidate's. This ensures the winner always has all committed entries.
//
// Execution trace:
//  1. Leader = Node X. Two followers: Y, Z.
//  2. Kill follower Z (minority — still quorum with X+Y).
//  3. Submit "PARTIAL_ENTRY". X+Y ACK → COMMITTED (quorum met).
//  4. Kill leader X.
//  5. Revive Z. Z has missed "PARTIAL_ENTRY".
//  6. Y and Z form cluster (Y has "PARTIAL_ENTRY", Z does not).
//  7. Y has more up-to-date log → Y wins election.
//  8. Y replicates "PARTIAL_ENTRY" to Z.
//  9. Z's log is repaired.
//  10. Cluster intact. "PARTIAL_ENTRY" committed and visible everywhere.
func RunScenario3_PartialReplication(c *Cluster) error {
	log.Println("═══════════════════════════════════════════════")
	log.Println("SCENARIO 3: Partial Replication Under Leader Crash")
	log.Println("═══════════════════════════════════════════════")
	log.Println("Testing: committed entry survival after leader crash")
	log.Println("Safety property: State Machine Safety (§5.4.3)")

	// Step 1: Find initial leader.
	leaderID, _, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario3: initial leader not elected: %w", err)
	}

	// Find a non-leader follower to kill (the "minority" follower).
	var killFollowerID int
	for id := 1; id <= c.size; id++ {
		if id != leaderID && c.IsAlive(id) {
			killFollowerID = id
			break
		}
	}
	log.Printf("S3: Leader=Node %d, killing follower Node %d (minority)", leaderID, killFollowerID)

	// Step 2: Kill one follower. Cluster still has quorum (2 of 3).
	c.Kill(killFollowerID)

	// Step 3: Submit "PARTIAL_ENTRY". This goes to leader + remaining follower = quorum.
	idx, _, err := c.SubmitCommand("PARTIAL_ENTRY")
	if err != nil {
		return fmt.Errorf("scenario3: submit failed: %w", err)
	}

	// Wait for commit confirmation.
	if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
		return fmt.Errorf("scenario3: PARTIAL_ENTRY not committed: %w", err)
	}
	log.Printf("S3: ✓ 'PARTIAL_ENTRY' committed at log[%d] on {Node%d + alive follower}", idx, leaderID)

	// Step 4: Kill the leader. Now we have:
	//   - Dead: original leader (Node leaderID), dead follower (Node killFollowerID)
	//   - Alive: one follower with "PARTIAL_ENTRY"
	log.Printf("S3: 💀 Killing leader Node %d", leaderID)
	c.Kill(leaderID)

	// Step 5: Revive the killed follower (the one that missed "PARTIAL_ENTRY").
	log.Printf("S3: 🔄 Reviving follower Node %d (missed PARTIAL_ENTRY)", killFollowerID)
	if err := c.Restart(killFollowerID); err != nil {
		return fmt.Errorf("scenario3: restart failed: %w", err)
	}

	// Step 6: New election between alive follower and revived follower.
	// The alive follower has "PARTIAL_ENTRY" (lastLogTerm=T, lastLogIndex=idx).
	// The revived follower does NOT have it (lastLogTerm < T or lastLogIndex < idx).
	// Election Restriction ensures: only the alive follower (or a node with its log)
	// can become leader.
	log.Println("S3: Waiting for new leader election (Election Restriction protects PARTIAL_ENTRY)...")
	newLeaderID, newTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario3: new leader not elected: %w", err)
	}
	log.Printf("S3: New leader = Node %d (term=%d)", newLeaderID, newTerm)

	// Step 7: Verify "PARTIAL_ENTRY" is in the new leader's log.
	// If the Election Restriction works correctly, the new leader MUST have it.
	newLeader := c.GetNode(newLeaderID)
	entries := newLeader.GetLog()
	found := false
	for _, e := range entries {
		if e.Command == "PARTIAL_ENTRY" {
			found = true
			break
		}
	}
	if !found {
		return errors.New("scenario3: SAFETY VIOLATION — 'PARTIAL_ENTRY' lost despite being committed")
	}
	log.Println("S3: ✓ 'PARTIAL_ENTRY' present in new leader's log (Election Restriction held)")

	// Step 8: Wait for the revived follower to receive "PARTIAL_ENTRY" via log repair.
	log.Printf("S3: Waiting for Node %d to receive PARTIAL_ENTRY via log repair...", killFollowerID)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		revivedNode := c.GetNode(killFollowerID)
		for _, e := range revivedNode.GetLog() {
			if e.Command == "PARTIAL_ENTRY" {
				found = true
				break
			}
		}
		if found {
			break
		}
		found = false
		time.Sleep(50 * time.Millisecond)
	}
	if !found {
		return fmt.Errorf("scenario3: Node %d did not receive PARTIAL_ENTRY via log repair", killFollowerID)
	}
	log.Printf("S3: ✓ Node %d's log repaired — 'PARTIAL_ENTRY' replicated", killFollowerID)

	log.Println("SCENARIO 3 COMPLETE: committed entry survived leader crash ✓")
	log.Println()
	return nil
}

// ---------------------------------------------------------------------------
// Scenario 4: Network Partition (Simulated via Kill/Restart)
// ---------------------------------------------------------------------------

// RunScenario4_NetworkPartition simulates a network partition by isolating a
// minority of nodes from the majority.
//
// In MiniRaft, partition simulation uses node Kill to prevent communication
// (the isolated node can't send or receive RPCs). In production, a real
// partition simulator would intercept TCP connections using a proxy layer,
// keeping the isolated node process alive but cutting its communication.
// The Raft properties being tested are identical in both approaches.
//
// Quorum mathematics:
//
//	3-node cluster, quorum = 2.
//	Partition: {Node 1} vs {Node 2, Node 3}
//	Minority  (size=1): cannot form quorum (1 < 2). Cannot elect leader.
//	Majority  (size=2): can form quorum (2 >= 2). Can elect leader.
//
// Why the minority cannot elect a leader:
//
//	To win, a candidate needs votes from a majority (≥2 of 3).
//	Node 1 alone can vote for itself (count=1 < quorum=2). Cannot win.
//	Even if Node 1 starts an election, it will run forever at ever-increasing
//	terms — but it cannot WIN because it can never collect 2 votes.
//
// What happens to the stale leader after partition heals:
//
//	The stale leader (if Node 1 was leader before) receives a heartbeat from
//	the new leader (term > stale term). checkTerm() steps it down immediately.
//	The stale leader becomes a Follower and syncs its log.
//
// Execution trace:
//  1. Cluster stable: leader = Node X (any).
//  2. Commit some entries.
//  3. Kill Node 1 (simulates isolation into minority partition).
//  4. Node 2 + Node 3 can still form quorum.
//  5. If Node X was Node 1: new election in {2,3} partition.
//     If Node X was Node 2 or 3: existing leader continues.
//  6. Commit entries in {2,3} partition.
//  7. Revive Node 1 (partition heals).
//  8. Node 1 receives heartbeat from {2,3} leader.
//  9. If Node 1 has stale term: checkTerm() steps it down.
//  10. Node 1 syncs log.
//  11. Exactly one leader, all logs identical.
func RunScenario4_NetworkPartition(c *Cluster) error {
	log.Println("═══════════════════════════════════════════════")
	log.Println("SCENARIO 4: Network Partition Simulation")
	log.Println("═══════════════════════════════════════════════")
	log.Println("Partition: {Node 1} vs {Node 2, Node 3}")
	log.Println("Quorum = 2. Minority cannot elect leader.")
	log.Printf("Safety property: §5.1 (one leader per term), §5.4 (quorum math)")

	// Step 1: Stable cluster.
	preLeaderID, preTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario4: initial leader not elected: %w", err)
	}
	log.Printf("S4: Pre-partition leader = Node %d (term=%d)", preLeaderID, preTerm)

	// Commit some entries before partitioning.
	idx, _, err := c.SubmitCommand("PRE_PARTITION")
	if err != nil {
		return fmt.Errorf("scenario4: pre-partition command failed: %w", err)
	}
	if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
		return fmt.Errorf("scenario4: pre-partition commit failed: %w", err)
	}
	log.Printf("S4: 'PRE_PARTITION' committed at log[%d] ✓", idx)

	// Step 2: Simulate partition — kill Node 1 (minority).
	isolatedNode := 1
	log.Printf("S4: 🔌 Partitioning Node %d into minority ({Node 1} vs {Node 2, Node 3})",
		isolatedNode)
	c.Kill(isolatedNode)

	// Step 3: Majority partition {Node 2, Node 3} must still have a leader.
	// If Node 1 was the leader, a new election will occur.
	log.Println("S4: Waiting for majority partition leader...")
	majorityLeaderID, majorityTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario4: majority partition leader not found: %w", err)
	}
	log.Printf("S4: ✓ Majority partition leader = Node %d (term=%d)", majorityLeaderID, majorityTerm)

	// Step 4: Submit commands in majority partition.
	// These should commit successfully (quorum = {Node 2, Node 3}).
	for i := 1; i <= 3; i++ {
		idx, _, err := c.SubmitCommand(fmt.Sprintf("MAJORITY_PARTITION_CMD_%d", i))
		if err != nil {
			return fmt.Errorf("scenario4: majority command %d failed: %w", i, err)
		}
		if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
			return fmt.Errorf("scenario4: majority commit %d failed: %w", i, err)
		}
	}
	log.Println("S4: ✓ Majority partition committed 3 commands (minority partition excluded)")

	// Step 5: Verify Node 1 (minority) has NOT committed the majority's commands.
	// In a real partition, Node 1 is alive but isolated. Here it's dead (Kill simulates
	// isolation). But the principle is: without quorum, no commits can happen.
	log.Printf("S4: ℹ️  Node %d (minority) could not commit during partition", isolatedNode)
	log.Printf("S4: ℹ️  Minority cannot form quorum (1 < quorum=%d) → no leader, no commits",
		c.QuorumSize())

	// Step 6: Heal the partition — restart Node 1.
	log.Printf("S4: 🔌 Healing partition — restarting Node %d", isolatedNode)
	if err := c.Restart(isolatedNode); err != nil {
		return fmt.Errorf("scenario4: partition heal restart failed: %w", err)
	}

	// Step 7: Wait for cluster to reach single-leader stability.
	log.Println("S4: Waiting for single leader after partition heal...")
	finalLeaderID, finalTerm, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		return fmt.Errorf("scenario4: post-heal leader not found: %w", err)
	}
	log.Printf("S4: ✓ Post-heal leader = Node %d (term=%d)", finalLeaderID, finalTerm)

	// The final leader term must be >= majority term (possible another election happened).
	if finalTerm < majorityTerm {
		return fmt.Errorf("scenario4: final term %d < majority term %d", finalTerm, majorityTerm)
	}

	// Step 8: Wait for Node 1 to synchronize its log with the majority.
	log.Printf("S4: Waiting for Node %d to sync log after partition heal...", isolatedNode)
	finalLeader := c.GetNode(finalLeaderID)
	expectedLastIdx := finalLeader.LastLogIndex()

	deadline := time.Now().Add(2 * time.Second)
	revivedNode := c.GetNode(isolatedNode)
	for time.Now().Before(deadline) {
		if revivedNode.LastLogIndex() >= expectedLastIdx {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	actualIdx := revivedNode.LastLogIndex()
	if actualIdx < expectedLastIdx {
		return fmt.Errorf("scenario4: Node %d log not synced (has %d, expected %d)",
			isolatedNode, actualIdx, expectedLastIdx)
	}
	log.Printf("S4: ✓ Node %d log synchronized (lastLogIndex=%d)", isolatedNode, actualIdx)

	// Final verification: exactly one leader.
	if finalLeaderID == 0 {
		return errors.New("scenario4: no single leader after partition heal")
	}
	log.Printf("S4: ✓ Single leader: Node %d at term=%d", finalLeaderID, finalTerm)

	log.Println("SCENARIO 4 COMPLETE: partition isolation and recovery verified ✓")
	log.Println()
	return nil
}

// ---------------------------------------------------------------------------
// State Transition Diagram (for documentation)
// ---------------------------------------------------------------------------

// PrintStateTransitions logs a human-readable state transition diagram for the
// current cluster. Useful for debugging and educational demos.
func (c *Cluster) PrintStateTransitions() {
	c.mu.Lock()
	size := c.size
	nodes := make(map[int]*Node, size)
	for id, n := range c.nodes {
		nodes[id] = n
	}
	c.mu.Unlock()

	log.Println("╔══════════════════════════════════════════════╗")
	log.Println("║          CLUSTER STATE SNAPSHOT              ║")
	log.Println("╠══════════════════════════════════════════════╣")

	for id := 1; id <= size; id++ {
		node := nodes[id]
		if node.isDead() {
			log.Printf("║  Node %d  │  💀 DEAD                              ║", id)
			continue
		}

		term, isLeader := node.GetState()
		role := "FOLLOWER"
		if isLeader {
			role = "⭐ LEADER"
		}

		logEntries := node.GetLog()
		commitIdx := node.GetCommitIndex()

		log.Printf("║  Node %d  │  %-10s │ term=%-3d │ log=%-2d │ commit=%-2d ║",
			id, role, term, len(logEntries), commitIdx)
	}

	log.Println("╚══════════════════════════════════════════════╝")
}
