package main

import (
	"sync"
	"testing"
	"time"
)

func TestMetricsElectionWin(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Leader should have ElectionsWon >= 1
	snap := c.GetNode(leaderID).GetMetricsSnapshot()
	if snap.ElectionsWon < 1 {
		t.Errorf("Node %d: expected ElectionsWon>=1, got %d", leaderID, snap.ElectionsWon)
	}
}

func TestMetricsRPCTracking(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	// Wait for a few heartbeats to happen
	time.Sleep(300 * time.Millisecond)

	snap := c.GetNode(leaderID).GetMetricsSnapshot()
	
	if snap.HeartbeatsSent == 0 {
		t.Errorf("expected HeartbeatsSent > 0, got %d", snap.HeartbeatsSent)
	}
	if snap.RPCSent <= snap.HeartbeatsSent {
		t.Errorf("expected RPCSent > HeartbeatsSent (due to RequestVotes), got %d <= %d", 
			snap.RPCSent, snap.HeartbeatsSent)
	}

	followerID := (leaderID % 3) + 1
	fsnap := c.GetNode(followerID).GetMetricsSnapshot()
	
	if fsnap.HeartbeatsReceived == 0 {
		t.Errorf("expected HeartbeatsReceived > 0, got %d", fsnap.HeartbeatsReceived)
	}
	if fsnap.RPCReceived <= fsnap.HeartbeatsReceived {
		t.Errorf("expected RPCReceived > HeartbeatsReceived (due to RequestVotes), got %d <= %d", 
			fsnap.RPCReceived, fsnap.HeartbeatsReceived)
	}
}

func TestMetricsCommandTracking(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	_, _ = mustWaitForLeader(t, c, 5*time.Second)

	// Submit 3 commands
	commands := []string{"SET A 1", "SET B 2", "SET C 3"}
	for _, cmd := range commands {
		idx, _ := mustSubmit(t, c, cmd)
		mustCommit(t, c, idx, 3*time.Second)
	}

	// Wait for followers to apply
	time.Sleep(200 * time.Millisecond)

	for id := 1; id <= 3; id++ {
		snap := c.GetNode(id).GetMetricsSnapshot()
		
		if snap.CommandsCommitted < 3 {
			t.Errorf("Node %d: expected CommandsCommitted >= 3, got %d", id, snap.CommandsCommitted)
		}
		if snap.CommandsApplied < 3 {
			t.Errorf("Node %d: expected CommandsApplied >= 3, got %d", id, snap.CommandsApplied)
		}
		if snap.CommitIndex < 3 {
			t.Errorf("Node %d: expected CommitIndex >= 3, got %d", id, snap.CommitIndex)
		}
	}
}

func TestMetricsSnapshot(t *testing.T) {
	setupDataDir(t)
	c := newTestCluster(t, 3)
	leaderID, term := mustWaitForLeader(t, c, 5*time.Second)

	snap := c.GetNode(leaderID).GetMetricsSnapshot()

	if snap.NodeID != leaderID {
		t.Errorf("expected NodeID %d, got %d", leaderID, snap.NodeID)
	}
	if snap.State != Leader {
		t.Errorf("expected State %s, got %s", Leader, snap.State)
	}
	if snap.CurrentTerm != term {
		t.Errorf("expected CurrentTerm %d, got %d", term, snap.CurrentTerm)
	}
	if snap.LogLength < 1 {
		t.Errorf("expected LogLength >= 1, got %d", snap.LogLength)
	}
}

func TestMetricsConcurrency(t *testing.T) {
	n := NewNode(1, map[int]string{})
	
	var wg sync.WaitGroup
	workers := 100
	incrementsPerWorker := 1000

	// 100 workers incrementing metrics concurrently
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				n.metrics.RPCSent.Add(1)
				n.metrics.RPCReceived.Add(1)
				n.metrics.CommandsApplied.Add(1)
			}
		}()
	}

	// Also have some workers capturing snapshots concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = n.GetMetricsSnapshot()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	expected := int64(workers * incrementsPerWorker)
	snap := n.GetMetricsSnapshot()

	if snap.RPCSent != expected {
		t.Errorf("expected RPCSent %d, got %d", expected, snap.RPCSent)
	}
	if snap.RPCReceived != expected {
		t.Errorf("expected RPCReceived %d, got %d", expected, snap.RPCReceived)
	}
	if snap.CommandsApplied != expected {
		t.Errorf("expected CommandsApplied %d, got %d", expected, snap.CommandsApplied)
	}
}
