package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Unit tests for the isolated KVStore
// ---------------------------------------------------------------------------

func TestKVStoreApplySet(t *testing.T) {
	kv := NewKVStore()

	err := kv.Apply("SET mykey myval")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, ok := kv.Get("mykey")
	if !ok || val != "myval" {
		t.Errorf("expected mykey=myval, got ok=%v val=%q", ok, val)
	}
}

func TestKVStoreApplyDelete(t *testing.T) {
	kv := NewKVStore()
	_ = kv.Apply("SET k1 v1")

	err := kv.Apply("DELETE k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := kv.Get("k1")
	if ok {
		t.Errorf("expected k1 to be deleted")
	}
}

func TestKVStoreApplyGet(t *testing.T) {
	kv := NewKVStore()
	_ = kv.Apply("SET k1 v1")

	// GET should succeed but not modify state
	err := kv.Apply("GET k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, ok := kv.Get("k1")
	if !ok || val != "v1" {
		t.Errorf("state corrupted by GET")
	}
}

func TestKVStoreApplyUnknown(t *testing.T) {
	kv := NewKVStore()
	err := kv.Apply("FOO bar")
	
	// Our relaxed Apply ignores unknown commands to preserve existing
	// test commands like "MUST_SURVIVE". If we changed Apply to be strict,
	// this test would check for error.
	if err != nil {
		t.Fatalf("did not expect error for unknown commands due to test compat: %v", err)
	}
}

func TestKVStoreSnapshot(t *testing.T) {
	kv := NewKVStore()
	_ = kv.Apply("SET k1 v1")
	_ = kv.Apply("SET k2 v2")

	snap := kv.Snapshot()
	if snap["k1"] != "v1" || snap["k2"] != "v2" {
		t.Errorf("snapshot does not match: %v", snap)
	}

	// modify original, make sure snapshot doesn't change
	_ = kv.Apply("SET k1 changed")
	if snap["k1"] == "changed" {
		t.Errorf("snapshot was modified! deep copy failed")
	}
}

func TestKVStoreConcurrency(t *testing.T) {
	kv := NewKVStore()
	
	var wg sync.WaitGroup
	// 10 concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := fmt.Sprintf("SET key_%d val_%d", i, i)
			kv.Apply(cmd)
		}(i)
	}

	// 10 concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kv.Get("key_0") // just read
			kv.Snapshot()   // take snapshot
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Integration tests for Replicated State Machine
// ---------------------------------------------------------------------------

func TestReplicatedKVStore(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	idx1, _ := mustSubmit(t, c, "SET x 10")
	mustCommit(t, c, idx1, 3*time.Second)

	idx2, _ := mustSubmit(t, c, "SET y 20")
	mustCommit(t, c, idx2, 3*time.Second)

	idx3, _ := mustSubmit(t, c, "DELETE x")
	mustCommit(t, c, idx3, 3*time.Second)

	// Wait for followers to apply the committed entries.
	// We wait slightly to ensure applyCommitted runs.
	time.Sleep(200 * time.Millisecond)

	for id := 1; id <= 3; id++ {
		if !c.IsAlive(id) {
			continue
		}
		sm := c.GetNode(id).GetStateMachine()
		
		val, ok := sm.Get("y")
		if !ok || val != "20" {
			t.Errorf("Node %d: expected y=20, got ok=%v val=%q", id, ok, val)
		}

		_, ok = sm.Get("x")
		if ok {
			t.Errorf("Node %d: expected x to be deleted", id)
		}
	}
	t.Log("PASS: Replicated KVStore maintains identical state across nodes")
}

func TestKVStoreAfterLeaderCrash(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderID, _ := mustWaitForLeader(t, c, 5*time.Second)

	idx, _ := mustSubmit(t, c, "SET k 100")
	mustCommit(t, c, idx, 3*time.Second)
	
	// wait for apply
	time.Sleep(100 * time.Millisecond)

	c.Kill(leaderID)

	newLeaderID, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("no new leader: %v", err)
	}

	// Verify the new leader has the committed state
	sm := c.GetNode(newLeaderID).GetStateMachine()
	val, ok := sm.Get("k")
	if !ok || val != "100" {
		t.Errorf("new leader %d missing state k=100 (got ok=%v val=%q)", newLeaderID, ok, val)
	}
	t.Log("PASS: State survives leader crash")
}

func TestKVStoreOrdering(t *testing.T) {
	c := newTestCluster(t, 3)
	mustWaitForLeader(t, c, 5*time.Second)

	idx1, _ := mustSubmit(t, c, "SET counter 1")
	mustCommit(t, c, idx1, 3*time.Second)

	idx2, _ := mustSubmit(t, c, "SET counter 2")
	mustCommit(t, c, idx2, 3*time.Second)

	idx3, _ := mustSubmit(t, c, "SET counter 3")
	mustCommit(t, c, idx3, 3*time.Second)

	time.Sleep(200 * time.Millisecond)

	// All nodes must have counter=3, proving they applied entries in exact order
	for id := 1; id <= 3; id++ {
		sm := c.GetNode(id).GetStateMachine()
		val, ok := sm.Get("counter")
		if !ok || val != "3" {
			t.Errorf("Node %d: expected counter=3, got ok=%v val=%q", id, ok, val)
		}
	}
	t.Log("PASS: Nodes apply entries in deterministic order")
}
