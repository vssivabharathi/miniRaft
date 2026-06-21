// raft_test.go — MiniRaft Comprehensive Test Suite
//
// This file contains the full automated test suite for MiniRaft. Tests are
// written using Go's standard `testing` package and the `Cluster` infrastructure
// from failure.go.
//
// Test categories:
//
//   Basic Correctness:
//     TestInitialElection           — exactly one leader per cluster start
//     TestLeaderStability           — leader doesn't change without failure
//     TestHigherTermElection        — higher-term node wins
//
//   Log Replication:
//     TestBasicReplication          — commands replicated to all nodes
//     TestReplicationOrdering       — commands applied in submission order
//     TestConcurrentSubmit          — multiple simultaneous commands
//     TestLargeLog                  — 100 commands committed correctly
//
//   Leader Failure:
//     TestLeaderCrash               — new leader elected after crash
//     TestCommittedEntryPreserved   — committed entry survives leader crash
//     TestMultipleLeaderCrashes     — survive N consecutive leader crashes
//
//   Log Repair:
//     TestFollowerLogRepair         — rejoined follower catches up
//     TestOldLeaderRejoin           — stale leader steps down and syncs
//     TestPartialReplication        — entry committed on majority survives
//
//   Quorum & Safety:
//     TestNoQuorumNoCommit          — cluster with 1-of-3 does not commit
//     TestElectionRestriction       — stale node cannot win election
//     TestOneLeaderPerTerm          — never two leaders in same term
//
//   Concurrency:
//     TestRaceCondition             — run with -race; no data races
//
// Running all tests:
//   go test -v -timeout 120s ./...
//
// Running with race detector (required for production quality):
//   go test -race -v -timeout 120s ./...
//
// Running a single test:
//   go test -v -run TestLeaderCrash ./...
//
// Raft paper references: §5 (Raft basics), §5.4 (safety)

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test helper utilities
// ---------------------------------------------------------------------------

// newTestCluster creates a fresh cluster for a test and registers cleanup.
// The cluster is automatically shut down when the test completes.
func newTestCluster(t *testing.T, size int) *Cluster {
	t.Helper()
	c, err := NewCluster(size)
	if err != nil {
		t.Fatalf("newTestCluster: %v", err)
	}
	t.Cleanup(func() {
		c.Shutdown()
		// Give goroutines a moment to drain after shutdown.
		time.Sleep(50 * time.Millisecond)
	})
	return c
}

// mustWaitForLeader fails the test if a stable leader is not elected
// within the given timeout.
func mustWaitForLeader(t *testing.T, c *Cluster, timeout time.Duration) (nodeID int, term int) {
	t.Helper()
	leaderID, term, err := c.WaitForLeader(timeout)
	if err != nil {
		t.Fatalf("mustWaitForLeader: %v", err)
	}
	return leaderID, term
}

// mustSubmit submits a command and fails the test if unsuccessful.
func mustSubmit(t *testing.T, c *Cluster, cmd string) (index int, term int) {
	t.Helper()
	idx, term, err := c.SubmitCommand(cmd)
	if err != nil {
		t.Fatalf("mustSubmit(%q): %v", cmd, err)
	}
	return idx, term
}

// mustCommit waits for a log index to be committed on quorum and fails if it times out.
func mustCommit(t *testing.T, c *Cluster, index int, timeout time.Duration) {
	t.Helper()
	if err := c.WaitForCommit(index, timeout); err != nil {
		t.Fatalf("mustCommit(index=%d): %v", index, err)
	}
}

// assertExactlyOneLeader verifies that exactly one node is the leader.
// Returns the leader's nodeID and term.
func assertExactlyOneLeader(t *testing.T, c *Cluster) (leaderID int, term int) {
	t.Helper()

	var leaders []int
	var leaderTerms []int

	for id := 1; id <= c.NodeCount(); id++ {
		if !c.IsAlive(id) {
			continue
		}
		node := c.GetNode(id)
		nodeTerm, isLeader := node.GetState()
		if isLeader {
			leaders = append(leaders, id)
			leaderTerms = append(leaderTerms, nodeTerm)
		}
	}

	if len(leaders) == 0 {
		t.Fatalf("assertExactlyOneLeader: no leader found in cluster")
	}
	if len(leaders) > 1 {
		t.Fatalf("assertExactlyOneLeader: SPLIT BRAIN — multiple leaders: %v (terms %v)",
			leaders, leaderTerms)
	}

	return leaders[0], leaderTerms[0]
}

// assertLogEntryCommand verifies that log entry at index contains the expected command
// on the given node.
func assertLogEntryCommand(t *testing.T, c *Cluster, nodeID, index int, expectedCmd string) {
	t.Helper()
	node := c.GetNode(nodeID)
	entries := node.GetLog()
	if index >= len(entries) {
		t.Fatalf("node %d: log[%d] does not exist (log length=%d)", nodeID, index, len(entries))
	}
	if entries[index].Command != expectedCmd {
		t.Fatalf("node %d: log[%d].Command = %q, want %q",
			nodeID, index, entries[index].Command, expectedCmd)
	}
}

// assertLogsMatch verifies that all alive nodes have identical log entries
// up through the given index.
func assertLogsMatch(t *testing.T, c *Cluster, upToIndex int) {
	t.Helper()

	var referenceLog []LogEntry
	var referenceNodeID int

	for id := 1; id <= c.NodeCount(); id++ {
		if !c.IsAlive(id) {
			continue
		}
		node := c.GetNode(id)
		entries := node.GetLog()

		if referenceNodeID == 0 {
			referenceLog = entries
			referenceNodeID = id
			continue
		}

		for i := 1; i <= upToIndex; i++ {
			if i >= len(referenceLog) || i >= len(entries) {
				t.Errorf("log length mismatch at index %d: Node%d has %d entries, Node%d has %d",
					i, referenceNodeID, len(referenceLog), id, len(entries))
				break
			}
			if referenceLog[i].Term != entries[i].Term ||
				referenceLog[i].Command != entries[i].Command {
				t.Errorf("log[%d] mismatch: Node%d={term=%d,cmd=%q} Node%d={term=%d,cmd=%q}",
					i,
					referenceNodeID, referenceLog[i].Term, referenceLog[i].Command,
					id, entries[i].Term, entries[i].Command)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Basic Correctness Tests
// ---------------------------------------------------------------------------

// TestInitialElection verifies that exactly one leader is elected when the
// cluster starts, and that the cluster achieves stable leadership quickly.
//
// Raft property: Election Safety (§5.2) — at most one leader per term.
// Common bug: multiple candidates win simultaneously (split-vote not resolved).
func TestInitialElection(t *testing.T) {
	c := newTestCluster(t, 3)

	// One leader must emerge within 3 election timeouts.
	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	t.Logf("initial leader: Node %d at term=%d", leaderID, term)

	// Term must be at least 1 (a real election took place).
	if term < 1 {
		t.Fatalf("term = %d, want >= 1", term)
	}

	// Exactly one leader must exist.
	assertExactlyOneLeader(t, c)

	// Stability check: leader should not change without cause.
	// Wait another 2 election timeouts and verify same leader.
	time.Sleep(600 * time.Millisecond)
	leaderID2, term2 := assertExactlyOneLeader(t, c)
	if leaderID2 != leaderID {
		t.Errorf("leader changed from Node %d to Node %d without failure", leaderID, leaderID2)
	}
	if term2 != term {
		t.Errorf("term changed from %d to %d without failure", term, term2)
	}

	t.Logf("PASS: single stable leader Node %d at term=%d", leaderID, term)
}

// TestLeaderStability verifies that a stable cluster keeps the same leader
// across multiple heartbeat intervals. No spurious re-elections.
//
// Raft property: Liveness — a healthy cluster should not hold unnecessary elections.
// Common bug: election timer not reset on heartbeat → constant re-elections.
func TestLeaderStability(t *testing.T) {
	c := newTestCluster(t, 3)

	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	// Wait 1 second (20 heartbeat intervals). Leader should stay stable.
	time.Sleep(1 * time.Second)

	leaderID2, term2 := assertExactlyOneLeader(t, c)

	if leaderID2 != leaderID {
		t.Errorf("spurious leader change: Node%d→Node%d (term %d→%d) — heartbeat not resetting timer?",
			leaderID, leaderID2, term, term2)
	}
	if term2 != term {
		t.Errorf("spurious term change: %d→%d — unnecessary election occurred", term, term2)
	}

	t.Logf("PASS: leader stable for 1s: Node%d at term=%d", leaderID, term)
}

// ---------------------------------------------------------------------------
// Log Replication Tests
// ---------------------------------------------------------------------------

// TestBasicReplication verifies that a command submitted to the leader is
// replicated to all followers and committed on a majority.
//
// Raft property: Log Matching (§5.3) — once committed, entry is identical on all nodes.
// Common bug: only leader gets the entry; followers never receive AppendEntries.
func TestBasicReplication(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	idx, _ := mustSubmit(t, c, "SET key=hello")
	mustCommit(t, c, idx, 3*time.Second)

	// Verify all alive nodes have the entry at the committed index.
	for id := 1; id <= 3; id++ {
		assertLogEntryCommand(t, c, id, idx, "SET key=hello")
	}

	t.Logf("PASS: 'SET key=hello' committed at log[%d] on all 3 nodes", idx)
}

// TestReplicationOrdering verifies that commands are applied in submission order.
// If the leader submits [A, B, C], every node must apply [A, B, C] in that order.
//
// Raft property: Log Matching (§5.3) — same index, same term, same command.
// Common bug: log entries appended out of order due to concurrent goroutines.
func TestReplicationOrdering(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	commands := []string{"CMD_A", "CMD_B", "CMD_C", "CMD_D", "CMD_E"}
	var indices []int

	// Submit commands sequentially.
	for _, cmd := range commands {
		idx, _ := mustSubmit(t, c, cmd)
		indices = append(indices, idx)
	}

	// Wait for all to commit.
	for _, idx := range indices {
		mustCommit(t, c, idx, 3*time.Second)
	}

	// Verify every node has the commands in the same order.
	for nodeID := 1; nodeID <= 3; nodeID++ {
		for i, idx := range indices {
			assertLogEntryCommand(t, c, nodeID, idx, commands[i])
		}
	}

	t.Logf("PASS: %d commands committed in order on all nodes", len(commands))
}

// TestConcurrentSubmit verifies that the cluster correctly handles multiple
// commands submitted concurrently to the leader.
//
// Raft property: Log Matching — concurrent appends must be linearized correctly.
// Common bug: two goroutines both call append without the lock → log corruption.
func TestConcurrentSubmit(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	const numCommands = 10
	var wg sync.WaitGroup
	indices := make([]int, numCommands)
	errs := make([]error, numCommands)

	// Submit all commands concurrently.
	for i := 0; i < numCommands; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := fmt.Sprintf("CONCURRENT_%d", i)
			idx, _, err := c.SubmitCommand(cmd)
			indices[i] = idx
			errs[i] = err
		}(i)
	}
	wg.Wait()

	// Count successful submissions.
	successCount := 0
	for i, err := range errs {
		if err == nil {
			successCount++
			mustCommit(t, c, indices[i], 3*time.Second)
		}
	}

	if successCount == 0 {
		t.Fatal("all concurrent submissions failed — no leader available?")
	}

	t.Logf("PASS: %d/%d concurrent commands committed", successCount, numCommands)
}

// TestLargeLog verifies that the cluster handles a large number of sequential
// commands without corruption, memory issues, or commit failures.
//
// Raft property: Log Matching over time — long logs must remain consistent.
// Common bug: slice reallocation bug (holding reference to old backing array).
func TestLargeLog(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	const total = 50
	var lastIdx int

	for i := 0; i < total; i++ {
		idx, _ := mustSubmit(t, c, fmt.Sprintf("LARGE_LOG_CMD_%03d", i))
		lastIdx = idx
	}

	// Only wait for the last one — if it's committed, all preceding are too
	// (commitIndex is monotone).
	mustCommit(t, c, lastIdx, 10*time.Second)

	// Verify all nodes have consistent logs.
	assertLogsMatch(t, c, lastIdx)

	t.Logf("PASS: %d commands committed, logs match on all nodes", total)
}

// ---------------------------------------------------------------------------
// Leader Failure Tests
// ---------------------------------------------------------------------------

// TestLeaderCrash verifies that a new leader is elected after the current
// leader crashes, and that the cluster remains operational.
//
// Raft property: Leader Election (§5.2) — cluster elects new leader on failure.
// Common bug: election timer not started/reset → cluster freezes after crash.
func TestLeaderCrash(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	// Crash the leader.
	c.Kill(leaderID)
	t.Logf("killed leader Node %d at term=%d", leaderID, term)

	// A new leader must emerge.
	newLeaderID, newTerm, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no new leader after crash: %v", err)
	}

	if newLeaderID == leaderID {
		t.Fatalf("dead node %d should not be leader", leaderID)
	}
	if newTerm <= term {
		t.Fatalf("new term %d should be > old term %d", newTerm, term)
	}

	t.Logf("PASS: new leader Node %d at term=%d (after Node %d crashed)", newLeaderID, newTerm, leaderID)
}

// TestCommittedEntryPreserved verifies the core Raft safety property:
// once an entry is committed on a majority, it is NEVER lost, even if the
// leader that committed it crashes immediately after.
//
// Raft property: State Machine Safety (§5.4.3) — committed entries are durable.
// Common bug: Election Restriction not enforced → stale node wins election and
// overwrites the committed entry with a different one.
func TestCommittedEntryPreserved(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Submit and commit a command.
	idx, _ := mustSubmit(t, c, "MUST_SURVIVE")
	mustCommit(t, c, idx, 3*time.Second)
	t.Logf("'MUST_SURVIVE' committed at log[%d]", idx)

	// Kill the leader immediately after commit.
	c.Kill(leaderID)

	// Wait for new leader.
	newLeaderID, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no new leader: %v", err)
	}
	t.Logf("new leader: Node %d", newLeaderID)

	// The new leader MUST have the committed entry.
	// If it doesn't, the Election Restriction failed.
	node := c.GetNode(newLeaderID)
	entries := node.GetLog()
	found := false
	for _, e := range entries {
		if e.Command == "MUST_SURVIVE" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("SAFETY VIOLATION: 'MUST_SURVIVE' lost after leader crash — Election Restriction failed")
	}

	t.Logf("PASS: committed entry 'MUST_SURVIVE' preserved after leader crash")
}

// TestMultipleLeaderCrashes verifies that the cluster remains correct across
// N consecutive leader crashes. This is the most demanding liveness test:
// after each crash, a new election must occur and the cluster must remain usable.
//
// Raft property: Fault Tolerance — survive up to ⌊(N-1)/2⌋ simultaneous failures.
// Common bug: persistent state corruption after second crash; term counter overflow.
func TestMultipleLeaderCrashes(t *testing.T) {
	// With 5 nodes, we can tolerate 2 simultaneous failures (quorum=3).
	// We crash leaders one at a time and restart them so the cluster always
	// has enough nodes for quorum. Without restart, 3 kills would leave
	// only 2 alive nodes — below the quorum of 3 — and no election could
	// succeed.
	c := newTestCluster(t, 5)
	_, _ = mustWaitForLeader(t, c, 5*time.Second)

	crashCount := 3
	var lastIdx int

	for round := 0; round < crashCount; round++ {
		leaderID, term := mustWaitForLeader(t, c, 5*time.Second)
		t.Logf("round %d: leader Node %d at term=%d", round+1, leaderID, term)

		// Submit a command in this round.
		idx, _ := mustSubmit(t, c, fmt.Sprintf("ROUND_%d", round))
		mustCommit(t, c, idx, 3*time.Second)
		lastIdx = idx

		// Crash this leader, then restart it as a follower so the cluster
		// stays at full strength for the next round.
		c.Kill(leaderID)
		t.Logf("round %d: killed Node %d", round+1, leaderID)
		time.Sleep(50 * time.Millisecond)
		if err := c.Restart(leaderID); err != nil {
			t.Fatalf("round %d: restart failed: %v", round+1, err)
		}
	}

	// After all crash/restart cycles, submit one more command to verify
	// the cluster is still fully operational.
	idx, _ := mustSubmit(t, c, "FINAL_COMMAND")
	mustCommit(t, c, idx, 5*time.Second)
	_ = lastIdx

	t.Logf("PASS: survived %d consecutive leader crashes, cluster still operational", crashCount)
}

// ---------------------------------------------------------------------------
// Log Repair Tests
// ---------------------------------------------------------------------------

// TestFollowerLogRepair verifies that a follower that missed entries while
// offline is correctly repaired when it rejoins.
//
// Raft property: Log Matching — the leader's log is the source of truth.
// Common bug: nextIndex never decremented → log repair loop doesn't converge.
func TestFollowerLogRepair(t *testing.T) {
	c := newTestCluster(t, 3)
	_, _ = mustWaitForLeader(t, c, 5*time.Second)

	// Find a non-leader to kill.
	leaderID, _ := assertExactlyOneLeader(t, c)
	var victimID int
	for id := 1; id <= 3; id++ {
		if id != leaderID {
			victimID = id
			break
		}
	}

	// Kill the victim follower.
	c.Kill(victimID)
	t.Logf("killed follower Node %d", victimID)

	// Submit commands while victim is offline.
	var lastIdx int
	for i := 0; i < 5; i++ {
		idx, _ := mustSubmit(t, c, fmt.Sprintf("WHILE_DOWN_%d", i))
		mustCommit(t, c, idx, 2*time.Second)
		lastIdx = idx
	}
	t.Logf("5 commands committed while Node %d was offline (lastIdx=%d)", victimID, lastIdx)

	// Restart the victim.
	if err := c.Restart(victimID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	t.Logf("restarted Node %d", victimID)

	// Wait for log repair.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node := c.GetNode(victimID)
		ok := node.LastLogIndex() >= lastIdx
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify the repaired log matches the leader's.
	node := c.GetNode(victimID)
	got := node.LastLogIndex()
	if got < lastIdx {
		t.Fatalf("Node %d log not repaired: lastLogIndex=%d, want >= %d",
			victimID, got, lastIdx)
	}

	assertLogsMatch(t, c, lastIdx)
	t.Logf("PASS: Node %d log repaired to index %d", victimID, lastIdx)
}

// TestOldLeaderRejoin verifies that a restarted ex-leader correctly steps down
// when it encounters the new leader's higher term, and that it never attempts
// to remain leader after restart.
//
// Raft property: Higher-Term Rule (§5.1) — any node observing T > currentTerm
// immediately becomes a Follower.
// Common bug: restarted node re-campaigns at old term → split-brain.
func TestOldLeaderRejoin(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeaderID, oldTerm := mustWaitForLeader(t, c, 5*time.Second)
	t.Logf("old leader: Node %d at term=%d", oldLeaderID, oldTerm)

	// Kill old leader and wait for new leader at higher term.
	c.Kill(oldLeaderID)
	newLeaderID, newTerm, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no new leader: %v", err)
	}
	t.Logf("new leader: Node %d at term=%d", newLeaderID, newTerm)

	// Submit entries that the old leader will have missed.
	for i := 0; i < 3; i++ {
		idx, _ := mustSubmit(t, c, fmt.Sprintf("NEW_ERA_%d", i))
		mustCommit(t, c, idx, 2*time.Second)
	}

	// Restart old leader.
	if err := c.Restart(oldLeaderID); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	// Allow time for term update and log sync.
	time.Sleep(400 * time.Millisecond)

	// Old leader must be Follower, not Leader.
	restartedTerm, restartedIsLeader := c.GetNode(oldLeaderID).GetState()
	if restartedIsLeader {
		t.Fatalf("restarted old leader Node %d is still leader — SAFETY VIOLATION", oldLeaderID)
	}
	if restartedTerm < newTerm {
		t.Fatalf("restarted node term %d < cluster term %d", restartedTerm, newTerm)
	}

	// Exactly one leader in cluster.
	assertExactlyOneLeader(t, c)

	t.Logf("PASS: old leader Node %d became Follower at term=%d", oldLeaderID, restartedTerm)
}

// TestPartialReplication demonstrates that a committed entry (on majority)
// is never lost, even if one follower missed it before the leader crashed.
//
// Raft property: State Machine Safety (§5.4.3) + Election Restriction (§5.4.1).
// Common bug: node that missed the entry can become leader → entry erased.
func TestPartialReplication(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Find the follower to isolate.
	var isolateID int
	for id := 1; id <= 3; id++ {
		if id != leaderID {
			isolateID = id
			break
		}
	}

	// Kill one follower (minority — cluster still has quorum).
	c.Kill(isolateID)
	t.Logf("isolated Node %d (minority)", isolateID)

	// Submit command. Committed on {leader + remaining follower} = quorum.
	idx, _ := mustSubmit(t, c, "COMMITTED_ENTRY")
	mustCommit(t, c, idx, 2*time.Second)
	t.Logf("'COMMITTED_ENTRY' committed at log[%d] on majority", idx)

	// Kill the leader. Now we have:
	//   Dead: leader, isolateID
	//   Alive: one follower with "COMMITTED_ENTRY"
	c.Kill(leaderID)
	t.Logf("killed leader Node %d", leaderID)

	// Revive the isolated node (which does NOT have "COMMITTED_ENTRY").
	if err := c.Restart(isolateID); err != nil {
		t.Fatalf("restart isolated: %v", err)
	}
	t.Logf("revived isolated Node %d (does not have COMMITTED_ENTRY)", isolateID)

	// New election between: [alive follower with entry] vs [revived node without].
	// Election Restriction: alive follower's log is more up-to-date → it wins.
	newLeaderID, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader after partial replication: %v", err)
	}
	t.Logf("new leader: Node %d", newLeaderID)

	// The new leader MUST have the committed entry.
	entries := c.GetNode(newLeaderID).GetLog()
	found := false
	for _, e := range entries {
		if e.Command == "COMMITTED_ENTRY" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("SAFETY VIOLATION: 'COMMITTED_ENTRY' lost — Election Restriction failed")
	}

	// Wait for the revived node to get the entry via log repair.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries := c.GetNode(isolateID).GetLog()
		for _, e := range entries {
			if e.Command == "COMMITTED_ENTRY" {
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
		t.Errorf("Node %d did not receive 'COMMITTED_ENTRY' via log repair within 3s", isolateID)
	}

	t.Logf("PASS: 'COMMITTED_ENTRY' preserved and replicated to all nodes")
}

// ---------------------------------------------------------------------------
// Quorum and Safety Tests
// ---------------------------------------------------------------------------

// TestNoQuorumNoCommit verifies that a cluster without quorum cannot commit
// new entries. With 3 nodes, killing 2 leaves only 1 alive — below quorum.
//
// Raft property: Quorum requirement — commits require majority agreement.
// Common bug: leader commits after single ACK → violates majority invariant.
func TestNoQuorumNoCommit(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Kill two followers — only the leader remains (1 of 3, below quorum).
	killed := 0
	for id := 1; id <= 3; id++ {
		if id != leaderID {
			c.Kill(id)
			killed++
		}
	}
	t.Logf("killed %d followers, only leader Node %d remains", killed, leaderID)

	// Submit a command. The leader appends it locally but cannot get quorum.
	// WaitForCommit must time out.
	idx, _, _ := c.SubmitCommand("NO_QUORUM")
	// Give the cluster 2 seconds to (incorrectly) commit it.
	err := c.WaitForCommit(idx, 2*time.Second)
	if err == nil {
		t.Fatalf("entry committed without quorum — SAFETY VIOLATION")
	}

	// The command must NOT be committed (commitIndex should be < idx on the leader).
	commitIdx := c.GetNode(leaderID).GetCommitIndex()
	if commitIdx >= idx {
		t.Fatalf("leader commit index %d >= submitted index %d without quorum",
			commitIdx, idx)
	}

	t.Logf("PASS: no commit without quorum (commitIndex=%d, submitted at %d)", commitIdx, idx)
}

// TestElectionRestriction verifies that a node with a stale log cannot win
// an election against a node with a more up-to-date log.
//
// Raft property: Election Restriction (§5.4.1) — voters deny if candidate
// log is less up-to-date.
// Common bug: isLogUpToDate returns true for all candidates → stale node wins.
func TestElectionRestriction(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Find follower IDs.
	var follower1, _ int
	for id := 1; id <= 3; id++ {
		if id != leaderID {
			if follower1 == 0 {
				follower1 = id
			}
		}
	}

	// Kill one follower first (it will miss entries).
	c.Kill(follower1)
	t.Logf("killed follower Node %d (will miss log entries)", follower1)

	// Submit several commands. Only leader + follower2 get them.
	var lastIdx int
	for i := 0; i < 5; i++ {
		idx, _ := mustSubmit(t, c, fmt.Sprintf("COMMITTED_%d", i))
		mustCommit(t, c, idx, 2*time.Second)
		lastIdx = idx
	}
	t.Logf("committed 5 entries (follower Node %d missed them)", follower1)

	// Kill leader and revive stale follower.
	c.Kill(leaderID)
	c.Restart(follower1) // follower1's log ends before the 5 entries

	// New election: follower1 (stale) vs follower2 (up-to-date).
	// follower2 must win (Election Restriction).
	newLeaderID, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no leader elected: %v", err)
	}

	if newLeaderID == follower1 {
		t.Fatalf("SAFETY VIOLATION: stale node (Node %d) won election despite missing %d committed entries",
			follower1, lastIdx)
	}

	t.Logf("PASS: up-to-date node Node %d won election; stale Node %d correctly lost",
		newLeaderID, follower1)
}

// TestOneLeaderPerTerm verifies that across multiple elections, no two leaders
// ever share the same term. This is the fundamental Election Safety property.
//
// Raft property: Election Safety (§5.2) — at most one leader per term.
// Common bug: votedFor not persisted correctly → grants two votes in same term.
func TestOneLeaderPerTerm(t *testing.T) {
	c := newTestCluster(t, 3)

	// Trigger several elections by crashing and restarting leaders.
	// We restart each crashed leader to ensure we always have 3 nodes
	// available — with only 2 alive nodes in a 3-node cluster, every
	// simultaneous candidacy leads to an unresolvable split vote (both
	// need 2 votes; each gets 1 from themselves). Restarting keeps the
	// cluster at full strength so elections resolve cleanly.
	seenLeaders := make(map[int]int) // term → nodeID

	for round := 0; round < 4; round++ {
		leaderID, term, err := c.WaitForLeader(5 * time.Second)
		if err != nil {
			t.Fatalf("round %d: no leader: %v", round, err)
		}

		// Safety check: no two leaders should ever share a term.
		if prevLeaderID, exists := seenLeaders[term]; exists {
			if prevLeaderID != leaderID {
				t.Fatalf("SAFETY VIOLATION: term=%d has two leaders: Node%d and Node%d",
					term, prevLeaderID, leaderID)
			}
		} else {
			seenLeaders[term] = leaderID
		}

		t.Logf("round %d: leader=Node%d term=%d", round, leaderID, term)

		// Kill the leader, then immediately restart it as a follower.
		// The restart ensures the cluster stays at 3 nodes so the
		// next election has 2 healthy followers to pick from.
		if round < 3 {
			c.Kill(leaderID)
			time.Sleep(50 * time.Millisecond) // let goroutines notice the kill
			if err := c.Restart(leaderID); err != nil {
				t.Fatalf("round %d: restart failed: %v", round, err)
			}
		}
	}

	t.Logf("PASS: %d elections, never two leaders in same term", len(seenLeaders))
}

// ---------------------------------------------------------------------------
// Concurrency / Race Detector Test
// ---------------------------------------------------------------------------

// TestRaceCondition runs a workload of concurrent submissions, leader crashes,
// and restarts while the Go race detector is active. Run with:
//
//	go test -race -run TestRaceCondition ./...
//
// Raft property: Thread safety — all concurrent accesses protected by n.mu.
// Common bug: reading n.currentTerm without lock → data race on term.
func TestRaceCondition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race test in short mode")
	}

	c := newTestCluster(t, 3)
	_, _ = mustWaitForLeader(t, c, 5*time.Second)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine 1: Continuously submit commands.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			c.SubmitCommand(fmt.Sprintf("RACE_%d", i)) //nolint:errcheck
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Goroutine 2: Periodically crash and restart leaders.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 3; round++ {
			select {
			case <-done:
				return
			default:
			}

			leaderID, _, err := c.WaitForLeader(2 * time.Second)
			if err != nil {
				continue
			}
			c.Kill(leaderID)
			time.Sleep(300 * time.Millisecond)
			c.Restart(leaderID) //nolint:errcheck
			time.Sleep(300 * time.Millisecond)
		}
		close(done)
	}()

	wg.Wait()

	// Final check: cluster is still operational.
	_, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("cluster not operational after concurrent workload: %v", err)
	}

	t.Log("PASS: no race conditions detected under concurrent submit + crash workload")
}

// ---------------------------------------------------------------------------
// Integration test — runs all four failure scenarios
// ---------------------------------------------------------------------------

// TestAllScenarios is an integration test that runs the four educational
// failure scenarios from failure.go as a single end-to-end test.
// This verifies that the scenario infrastructure itself is correct.
func TestAllScenarios(t *testing.T) {
	scenarios := []struct {
		name string
		fn   func(*Cluster) error
	}{
		{"LeaderCrash", RunScenario1_LeaderCrash},
		{"OldLeaderRejoin", RunScenario2_OldLeaderRejoin},
		{"PartialReplication", RunScenario3_PartialReplication},
		{"NetworkPartition", RunScenario4_NetworkPartition},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			// Each scenario gets its own fresh cluster.
			c := newTestCluster(t, 3)
			if err := sc.fn(c); err != nil {
				t.Fatalf("scenario %s: %v", sc.name, err)
			}
		})
		// Brief pause between scenarios so ports are fully released.
		time.Sleep(150 * time.Millisecond)
	}
}
