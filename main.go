// main.go — MiniRaft Cluster Bootstrap and Live Demonstration
//
// This is the entry point for the MiniRaft cluster. It:
//
//   1. Bootstraps a 3-node Raft cluster on localhost:5001–5003.
//   2. Waits for the initial leader election.
//   3. Runs a live command submission demo.
//   4. Runs the four failure-recovery scenarios from failure.go.
//   5. Prints a final cluster state snapshot.
//
// Running:
//   go run .
//   OR
//   go build -o miniRaft && ./miniRaft
//
// Ports used:
//   Node 1 → localhost:5001
//   Node 2 → localhost:5002
//   Node 3 → localhost:5003
//
// Log format:
//   [Node N][Term T][STATE] message
//   [Cluster] message
//
// The output is intentionally verbose for educational purposes.
// Each line of output corresponds to a specific Raft protocol event.
// Reading the log top-to-bottom gives a complete trace of the consensus process.

package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// Configure logging: include timestamp and no source file prefix.
	// We add our own [Node N][Term T] prefix in every log line.
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stdout)

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    MiniRaft — Phase 7                       ║")
	fmt.Println("║          3-Node Raft Cluster — Live Demonstration            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// -----------------------------------------------------------------------
	// BOOTSTRAP: Create the 3-node cluster.
	//
	// NewCluster:
	//   1. Creates Node structs with peer maps.
	//   2. Starts RPC servers (listeners on :5001, :5002, :5003).
	//   3. Starts election tickers on all nodes.
	//
	// After NewCluster returns, the first election will occur within
	// 150–300ms (randomized election timeout).
	// -----------------------------------------------------------------------
	log.Println("Bootstrapping 3-node MiniRaft cluster...")
	cluster, err := NewCluster(3)
	if err != nil {
		log.Fatalf("FATAL: failed to create cluster: %v", err)
	}
	defer cluster.Shutdown()

	// -----------------------------------------------------------------------
	// PHASE A: Wait for initial leader election.
	// -----------------------------------------------------------------------
	log.Println()
	log.Println("── Phase A: Initial Leader Election ────────────────────────────")
	leaderID, term, err := cluster.WaitForLeader(5 * time.Second)
	if err != nil {
		log.Fatalf("FATAL: no leader elected: %v", err)
	}
	log.Printf("✓ Leader elected: Node %d at term=%d", leaderID, term)
	cluster.PrintStateTransitions()

	// -----------------------------------------------------------------------
	// PHASE B: Live command submission demo.
	//
	// Submit 5 commands to the cluster. Each command goes through:
	//   Client → Leader.SubmitCommand → log append → replicate to followers
	//   → majority ACK → commitIndex advance → applyCommitted
	// -----------------------------------------------------------------------
	log.Println()
	log.Println("── Phase B: Command Submission Demo ────────────────────────────")

	commands := []string{"SET x=1", "SET y=2", "SET z=3", "INCR x", "DEL y"}
	for _, cmd := range commands {
		idx, cmdTerm, err := cluster.SubmitCommand(cmd)
		if err != nil {
			log.Printf("  WARN: command %q failed: %v", cmd, err)
			continue
		}

		// Wait for the command to be committed on a majority.
		if err := cluster.WaitForCommit(idx, 2*time.Second); err != nil {
			log.Printf("  WARN: commit of %q at log[%d] timed out: %v", cmd, idx, err)
			continue
		}
		log.Printf("  ✓ '%s' → committed at log[%d] term=%d", cmd, idx, cmdTerm)
	}

	log.Println()
	cluster.PrintStateTransitions()

	// -----------------------------------------------------------------------
	// PHASE C: Failure scenario demonstrations.
	//
	// Each scenario tests a specific Raft safety or liveness property.
	// See failure.go for the full educational documentation.
	// -----------------------------------------------------------------------
	log.Println()
	log.Println("── Phase C: Failure Recovery Scenarios ─────────────────────────")

	// We restart the cluster for each scenario to get a clean state.
	// Scenario 1: Leader Crash
	log.Println()
	s1Cluster, err := NewCluster(3)
	if err != nil {
		log.Fatalf("scenario1 cluster: %v", err)
	}
	if err := RunScenario1_LeaderCrash(s1Cluster); err != nil {
		log.Printf("SCENARIO 1 FAILED: %v", err)
	}
	s1Cluster.Shutdown()
	time.Sleep(100 * time.Millisecond) // let ports release

	// Scenario 2: Old Leader Rejoin
	s2Cluster, err := NewCluster(3)
	if err != nil {
		log.Fatalf("scenario2 cluster: %v", err)
	}
	if err := RunScenario2_OldLeaderRejoin(s2Cluster); err != nil {
		log.Printf("SCENARIO 2 FAILED: %v", err)
	}
	s2Cluster.Shutdown()
	time.Sleep(100 * time.Millisecond)

	// Scenario 3: Partial Replication
	s3Cluster, err := NewCluster(3)
	if err != nil {
		log.Fatalf("scenario3 cluster: %v", err)
	}
	if err := RunScenario3_PartialReplication(s3Cluster); err != nil {
		log.Printf("SCENARIO 3 FAILED: %v", err)
	}
	s3Cluster.Shutdown()
	time.Sleep(100 * time.Millisecond)

	// Scenario 4: Network Partition
	s4Cluster, err := NewCluster(3)
	if err != nil {
		log.Fatalf("scenario4 cluster: %v", err)
	}
	if err := RunScenario4_NetworkPartition(s4Cluster); err != nil {
		log.Printf("SCENARIO 4 FAILED: %v", err)
	}
	s4Cluster.Shutdown()

	// -----------------------------------------------------------------------
	// PHASE D: Final summary.
	// -----------------------------------------------------------------------
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                  MiniRaft Demo Complete                      ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Phases completed:                                           ║")
	fmt.Println("║    Phase 1: Architecture & data structures          ✓        ║")
	fmt.Println("║    Phase 2: node.go  — Core node & state machine   ✓        ║")
	fmt.Println("║    Phase 3: election.go — Leader election           ✓        ║")
	fmt.Println("║    Phase 4: heartbeat.go — Leader heartbeats        ✓        ║")
	fmt.Println("║    Phase 5: rpc.go — Full log replication           ✓        ║")
	fmt.Println("║    Phase 6: failure.go — Failure & recovery         ✓        ║")
	fmt.Println("║    Phase 7: main.go — Cluster bootstrap             ✓        ║")
	fmt.Println("║    Phase 8: *_test.go — Comprehensive tests         ✓        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
}
