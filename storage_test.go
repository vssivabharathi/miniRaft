package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// setupDataDir ensures tests run with a clean data directory.
func setupDataDir(t testing.TB) {
	os.RemoveAll("data")
	err := os.MkdirAll("data", 0755)
	if err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll("data")
	})
}

func TestPersistenceBasic(t *testing.T) {
	setupDataDir(t)
	n := NewNode(1, map[int]string{})
	
	// Modify state manually and persist
	n.mu.Lock()
	n.currentTerm = 5
	n.votedFor = 2
	n.log = append(n.log, LogEntry{Index: 1, Term: 5, Command: "SET X 1"})
	n.mu.Unlock()

	err := n.persist()
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// Verify file exists
	filename := fmt.Sprintf("data/node-%d.state", n.id)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("expected file %s to exist", filename)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	idx, _ := mustSubmit(t, c, "SET x 10")
	mustCommit(t, c, idx, 3*time.Second)

	// Wait for replication and persistence
	time.Sleep(200 * time.Millisecond)

	for id := 1; id <= 3; id++ {
		c.Kill(id)
	}

	for id := 1; id <= 3; id++ {
		c.Restart(id)
	}

	// The log should be intact on all revived nodes
	for id := 1; id <= 3; id++ {
		log := c.GetNode(id).GetLog()
		if len(log) < 2 {
			t.Errorf("Node %d: expected log length >= 2, got %d", id, len(log))
		}
		if len(log) >= 2 && log[1].Command != "SET x 10" {
			t.Errorf("Node %d: expected log[1] to be 'SET x 10', got %q", id, log[1].Command)
		}
	}
}

func TestTermPersistence(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	// Kill leader to force term advancement
	c.Kill(leaderID)
	
	newLeaderID, newTerm, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no new leader: %v", err)
	}
	if newTerm <= term {
		t.Fatalf("expected new term > %d, got %d", term, newTerm)
	}

	// Wait for followers to persist their new term via heartbeat/AppendEntries
	time.Sleep(200 * time.Millisecond)

	c.Kill(newLeaderID)
	c.Restart(newLeaderID)

	termAfterRevive, _ := c.GetNode(newLeaderID).GetState()
	if termAfterRevive != newTerm {
		t.Errorf("expected term %d after revive, got %d", newTerm, termAfterRevive)
	}
}

func TestVotePersistence(t *testing.T) {
	setupDataDir(t)
	n := NewNode(1, map[int]string{2: "foo"})
	n.mu.Lock()
	n.votedFor = 2
	n.mu.Unlock()
	
	n.persist()
	
	// Create a new node with the same ID, it should restore the vote
	n2 := NewNode(1, map[int]string{2: "foo"})
	
	n2.mu.RLock()
	votedFor := n2.votedFor
	n2.mu.RUnlock()
	
	if votedFor != 2 {
		t.Errorf("expected restored votedFor to be 2, got %d", votedFor)
	}
}

func TestLogPersistence(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Submit multiple commands
	commands := []string{"CMD 1", "CMD 2", "CMD 3"}
	for _, cmd := range commands {
		idx, _ := mustSubmit(t, c, cmd)
		mustCommit(t, c, idx, 3*time.Second)
	}

	time.Sleep(200 * time.Millisecond) // Ensure followers persist
	
	followerID := (leaderID % 3) + 1
	c.Kill(followerID)
	
	// Submit more commands while follower is down
	idx, _ := mustSubmit(t, c, "CMD 4")
	mustCommit(t, c, idx, 3*time.Second)

	c.Restart(followerID)
	
	// Give it time to catch up
	time.Sleep(500 * time.Millisecond)

	log := c.GetNode(followerID).GetLog()
	
	expectedCount := 5 // sentinel + 4 commands
	if len(log) != expectedCount {
		t.Errorf("expected log length %d, got %d", expectedCount, len(log))
	}
}

func TestMultipleRestarts(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	
	for i := 0; i < 3; i++ {
		_, _ = mustWaitForLeader(t, c, 5*time.Second)
		cmd := fmt.Sprintf("ITERATION %d", i)
		idx, _ := mustSubmit(t, c, cmd)
		mustCommit(t, c, idx, 3*time.Second)
		
		time.Sleep(100 * time.Millisecond) // flush I/O
		
		// Crash the whole cluster!
		for id := 1; id <= 3; id++ {
			c.Kill(id)
		}
		
		// Wait a bit
		time.Sleep(200 * time.Millisecond)
		
		// Revive everyone
		for id := 1; id <= 3; id++ {
			c.Restart(id)
		}
	}
	
	// Final check
	mustWaitForLeader(t, c, 5*time.Second)
	
	for id := 1; id <= 3; id++ {
		log := c.GetNode(id).GetLog()
		// sentinel + 3 iterations
		if len(log) != 4 {
			t.Errorf("Node %d: expected log length 4, got %d", id, len(log))
		}
	}
}
