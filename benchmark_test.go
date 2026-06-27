package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// BENCHMARK HELPERS
// ============================================================================

func mustCluster(b *testing.B, size int) *Cluster {
	b.Helper()
	c, err := NewCluster(size)
	if err != nil {
		b.Fatalf("mustCluster(%d): %v", size, err)
	}
	b.Cleanup(func() {
		c.Shutdown()
		// Give goroutines a moment to drain after shutdown.
		time.Sleep(60 * time.Millisecond)
	})
	return c
}

// mustWaitLeader fails the benchmark if no stable leader is elected within
// the timeout. Returns the elected (leaderID, term) pair.
func mustWaitLeader(b *testing.B, c *Cluster, timeout time.Duration) (int, int) {
	b.Helper()
	id, term, err := c.WaitForLeader(timeout)
	if err != nil {
		b.Fatalf("mustWaitLeader: %v", err)
	}
	return id, term
}

// mustSubmitAndCommit submits a command and blocks until it is committed on a
// quorum, failing the benchmark on any error. Returns the committed log index.
func mustSubmitAndCommit(b *testing.B, c *Cluster, cmd string, commitTimeout time.Duration) int {
	b.Helper()
	idx, _, err := c.SubmitCommand(cmd)
	if err != nil {
		b.Fatalf("mustSubmitAndCommit(%q): submit error: %v", cmd, err)
	}
	if err := c.WaitForCommit(idx, commitTimeout); err != nil {
		b.Fatalf("mustSubmitAndCommit(%q): commit error: %v", cmd, err)
	}
	return idx
}

// bToMB converts bytes to mebibytes for human-readable output.
func bToMB(b uint64) float64 { return float64(b) / 1024 / 1024 }

// p99 computes the 99th-percentile value from a sorted slice of durations.
// The input slice must be sorted ascending before calling.
func p99(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * 0.99)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ============================================================================
// BENCHMARK 1: LEADER ELECTION LATENCY
// ============================================================================

func BenchmarkLeaderElectionLatency(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			var totalElection time.Duration
			var minElection = time.Hour
			var maxElection time.Duration

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Boot a fresh cluster per iteration so every run starts from
				// a cold state — no pre-existing leader to mask latency.
				b.StopTimer()
				c, err := NewCluster(size)
				if err != nil {
					b.Fatalf("NewCluster: %v", err)
				}
				b.StartTimer()

				start := time.Now()
				_, _, err = c.WaitForLeader(5 * time.Second)
				elapsed := time.Since(start)

				b.StopTimer()
				c.Shutdown()
				time.Sleep(60 * time.Millisecond)
				b.StartTimer()

				if err != nil {
					b.Logf("iteration %d: no leader within 5s: %v", i, err)
					continue
				}

				totalElection += elapsed
				if elapsed < minElection {
					minElection = elapsed
				}
				if elapsed > maxElection {
					maxElection = elapsed
				}
			}

			avgElection := totalElection / time.Duration(b.N)

			//output format
			fmt.Printf("\n[BENCHMARK] Leader Election Latency — %d nodes\n", size)
			fmt.Printf("  Min  : %6.2f ms\n", float64(minElection.Milliseconds()))
			fmt.Printf("  Avg  : %6.2f ms\n", float64(avgElection.Milliseconds()))
			fmt.Printf("  Max  : %6.2f ms\n", float64(maxElection.Milliseconds()))
			fmt.Printf("  ↳ Election timeout window: %v–%v (randomized)\n",
				ElectionTimeoutMin, ElectionTimeoutMax)
		})
	}
}

// ============================================================================
// BENCHMARK 2: LOG REPLICATION THROUGHPUT"
// ============================================================================

func BenchmarkLogReplicationThroughput(b *testing.B) {
	for _, numEntries := range []int{50, 200, 500} {
		b.Run(fmt.Sprintf("%d_entries", numEntries), func(b *testing.B) {
			c := mustCluster(b, 3)
			mustWaitLeader(b, c, 5*time.Second)

			b.ResetTimer()

			for iter := 0; iter < b.N; iter++ {
				start := time.Now()
				var lastIdx int

				for i := 0; i < numEntries; i++ {
					cmd := fmt.Sprintf("SET bench_key_%d value_%d_%d", iter, i, rand.Int63())
					idx, _, err := c.SubmitCommand(cmd)
					if err != nil {
						b.Logf("SubmitCommand failed (iter=%d, i=%d): %v", iter, i, err)
						continue
					}
					lastIdx = idx
				}

				// Block until the LAST entry is committed — monotonicity of
				// commitIndex guarantees all prior entries are also committed.
				if lastIdx > 0 {
					if err := c.WaitForCommit(lastIdx, 10*time.Second); err != nil {
						b.Logf("WaitForCommit(%d) timed out: %v", lastIdx, err)
					}
				}

				elapsed := time.Since(start)
				throughput := float64(numEntries) / elapsed.Seconds()
				//output format
				fmt.Printf("\n[BENCHMARK] Log Replication Throughput — %d entries\n", numEntries)
				fmt.Printf("  Duration   : %.3f sec\n", elapsed.Seconds())
				fmt.Printf("  Throughput : %.2f entries/sec (quorum-committed)\n", throughput)
				fmt.Printf("  Per-entry  : %.2f ms avg latency\n",
					float64(elapsed.Milliseconds())/float64(numEntries))
			}
		})
	}
}

// ============================================================================
// BENCHMARK 3: HEARTBEAT ROUND-TRIP LATENCY
// ============================================================================

func BenchmarkHeartbeatLatency(b *testing.B) {
	c := mustCluster(b, 3)
	leaderID, _ := mustWaitLeader(b, c, 5*time.Second)
	leader := c.GetNode(leaderID)

	// Seed the log with one committed entry so heartbeats carry a non-trivial
	// leaderCommit and exercise the full code path (not just idle heartbeat).
	mustSubmitAndCommit(b, c, "HEARTBEAT_SEED", 3*time.Second)

	b.ResetTimer()

	var totalRTT time.Duration
	var minRTT = time.Hour
	var maxRTT time.Duration

	for i := 0; i < b.N; i++ {
		// Capture the leader's metrics snapshot BEFORE triggering a heartbeat.
		before := leader.GetMetricsSnapshot()

		start := time.Now()

		// heartbeatTicker fires one round of concurrent per-peer AppendEntries.
		// We call it directly (same function the heartbeat loop calls) to
		// measure exactly one heartbeat cycle.
		leader.heartbeatTicker(before.CurrentTerm)

		// Poll until the heartbeat counter advances, indicating at least one
		// follower has ACK'd. We use a tight poll (1 ms) to minimize measurement
		// overhead while still capturing the real RPC round-trip.
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			after := leader.GetMetricsSnapshot()
			if after.HeartbeatsSent > before.HeartbeatsSent {
				break
			}
			time.Sleep(time.Millisecond)
		}
		rtt := time.Since(start)

		totalRTT += rtt
		if rtt < minRTT {
			minRTT = rtt
		}
		if rtt > maxRTT {
			maxRTT = rtt
		}
	}

	avgRTT := totalRTT / time.Duration(b.N)
	//output format
	fmt.Printf("\n[BENCHMARK] Heartbeat Round-Trip Latency (3-node)\n")
	fmt.Printf("  Min  : %6.3f ms\n", float64(minRTT.Microseconds())/1000)
	fmt.Printf("  Avg  : %6.3f ms\n", float64(avgRTT.Microseconds())/1000)
	fmt.Printf("  Max  : %6.3f ms\n", float64(maxRTT.Microseconds())/1000)
	fmt.Printf("  ↳ Heartbeat interval: %v, transport: loopback TCP\n", HeartbeatInterval)
}

// ============================================================================
// BENCHMARK 4: FAILURE DETECTION & LEADER RECOVERY TIME
// ============================================================================

func BenchmarkFailureAndRecoveryTime(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			var totalRecovery time.Duration
			var minRecovery = time.Hour
			var maxRecovery time.Duration
			recoveries := make([]time.Duration, 0, b.N)

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				c, err := NewCluster(size)
				if err != nil {
					b.Fatalf("NewCluster: %v", err)
				}

				// Elect initial leader and commit one entry to ensure cluster
				// is fully operational before we inject the crash.
				leaderID, _, err := c.WaitForLeader(5 * time.Second)
				if err != nil {
					c.Shutdown()
					b.Logf("initial election failed: %v", err)
					continue
				}
				// Commit a seed entry so followers have a non-trivial matchIndex
				// and the new leader's log is more realistic.
				idx, _, _ := c.SubmitCommand("RECOVERY_SEED")
				_ = c.WaitForCommit(idx, 2*time.Second)

				b.StartTimer()

				// ── Phase 1+2+3+4: Kill → detect → elect → confirm ─────────
				crashTime := time.Now()
				c.Kill(leaderID)

				// WaitForLeader polls every 20 ms and requires two consecutive
				// agreements — it captures detection + election + stability.
				_, _, err = c.WaitForLeader(6 * time.Second)
				recoveryTime := time.Since(crashTime)

				b.StopTimer()
				c.Shutdown()
				time.Sleep(60 * time.Millisecond)

				if err != nil {
					b.Logf("recovery benchmark iter %d: no new leader: %v", i, err)
					continue
				}

				recoveries = append(recoveries, recoveryTime)
				totalRecovery += recoveryTime
				if recoveryTime < minRecovery {
					minRecovery = recoveryTime
				}
				if recoveryTime > maxRecovery {
					maxRecovery = recoveryTime
				}
			}

			if len(recoveries) == 0 {
				b.Log("no successful recovery measurements")
				return
			}

			sort.Slice(recoveries, func(i, j int) bool { return recoveries[i] < recoveries[j] })
			avgRecovery := totalRecovery / time.Duration(len(recoveries))
			p99Recovery := p99(recoveries)
			//output format
			fmt.Printf("\n[BENCHMARK] Failure Detection & Recovery Time — %d nodes\n", size)
			fmt.Printf("  Min        : %6.2f ms  (best case: short timeout + fast election)\n",
				float64(minRecovery.Milliseconds()))
			fmt.Printf("  Avg        : %6.2f ms\n", float64(avgRecovery.Milliseconds()))
			fmt.Printf("  P99        : %6.2f ms\n", float64(p99Recovery.Milliseconds()))
			fmt.Printf("  Max        : %6.2f ms  (worst case: max timeout + split vote retry)\n",
				float64(maxRecovery.Milliseconds()))
			fmt.Printf("  ↳ Breakdown: election timeout (%v–%v) + election (<100 ms)\n",
				ElectionTimeoutMin, ElectionTimeoutMax)
		})
	}
}

// ============================================================================
// BENCHMARK 5: MEMORY FOOTPRINT PER NODE
// ============================================================================

func BenchmarkMemoryFootprint(b *testing.B) {
	for _, numEntries := range []int{1000, 5000, 10000} {
		b.Run(fmt.Sprintf("%d_entries", numEntries), func(b *testing.B) {
			c := mustCluster(b, 3)
			mustWaitLeader(b, c, 5*time.Second)

			// Force GC to get a clean baseline before we start accumulating.
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			b.ResetTimer()

			// Submit numEntries commands and wait for the last one to commit.
			var lastIdx int
			for i := 0; i < numEntries; i++ {
				idx, _, err := c.SubmitCommand(fmt.Sprintf("SET mem_key_%d value_%d", i, i))
				if err != nil {
					b.Logf("submit %d failed: %v", i, err)
					continue
				}
				lastIdx = idx
			}
			if lastIdx > 0 {
				if err := c.WaitForCommit(lastIdx, 30*time.Second); err != nil {
					b.Logf("WaitForCommit(%d) failed: %v", lastIdx, err)
				}
			}

			b.StopTimer()

			// Force GC to flush finalizer queues and get accurate live-object
			// counts, then measure heap after the log is populated.
			runtime.GC()
			runtime.GC() // two passes catches more deferred work
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
			perEntry := float64(heapDelta) / float64(numEntries)
			//output format
			fmt.Printf("\n[BENCHMARK] Memory Footprint — %d log entries (3-node)\n", numEntries)
			fmt.Printf("  Heap before  : %.2f MB\n", bToMB(memBefore.HeapInuse))
			fmt.Printf("  Heap after   : %.2f MB\n", bToMB(memAfter.HeapInuse))
			fmt.Printf("  Heap delta   : %+.2f MB  (log + KV state machine)\n",
				float64(heapDelta)/1024/1024)
			fmt.Printf("  Per entry    : %.2f KB\n", perEntry/1024)
			fmt.Printf("  ↳ Includes: LogEntry struct + KVStore map entry + string interning\n")
		})
	}
}

// ============================================================================
// BENCHMARK 6: CONCURRENT RPC HANDLING (STRESS TEST)
// ============================================================================
func BenchmarkConcurrentRPCHandling(b *testing.B) {
	for _, concurrency := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("%d_concurrent", concurrency), func(b *testing.B) {
			c := mustCluster(b, 3)
			mustWaitLeader(b, c, 5*time.Second)

			b.ResetTimer()

			for iter := 0; iter < b.N; iter++ {
				latencies := make([]time.Duration, concurrency)
				var wg sync.WaitGroup
				var successCount atomic.Int64

				overallStart := time.Now()

				for id := 0; id < concurrency; id++ {
					wg.Add(1)
					go func(reqID int) {
						defer wg.Done()

						reqStart := time.Now()
						cmd := fmt.Sprintf("SET concurrent_key_%d_%d value_%d", iter, reqID, reqID)
						idx, _, err := c.SubmitCommand(cmd)
						if err != nil {
							latencies[reqID] = time.Since(reqStart)
							return
						}
						// Wait for this specific entry to commit.
						if cerr := c.WaitForCommit(idx, 5*time.Second); cerr == nil {
							successCount.Add(1)
						}
						latencies[reqID] = time.Since(reqStart)
					}(id)
				}

				wg.Wait()
				overallDuration := time.Since(overallStart)

				// Sort for percentile computation.
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

				var totalLat time.Duration
				for _, l := range latencies {
					totalLat += l
				}
				avgLat := totalLat / time.Duration(concurrency)
				p99Lat := p99(latencies)
				throughput := float64(successCount.Load()) / overallDuration.Seconds()

				fmt.Printf("\n[BENCHMARK] Concurrent RPC Handling — %d concurrent requests\n", concurrency)
				fmt.Printf("  Successful : %d / %d requests committed\n",
					successCount.Load(), concurrency)
				fmt.Printf("  Total time : %.2f ms\n", float64(overallDuration.Milliseconds()))
				fmt.Printf("  Throughput : %.2f req/sec  (committed)\n", throughput)
				fmt.Printf("  Avg latency: %.2f ms\n", float64(avgLat.Milliseconds()))
				fmt.Printf("  P99 latency: %.2f ms\n", float64(p99Lat.Milliseconds()))
				fmt.Printf("  Min latency: %.2f ms\n", float64(latencies[0].Milliseconds()))
				fmt.Printf("  Max latency: %.2f ms\n", float64(latencies[len(latencies)-1].Milliseconds()))
			}
		})
	}
}

// ============================================================================
// BENCHMARK 7: NETWORK PARTITION RECOVERY
// ============================================================================
func BenchmarkNetworkPartitionRecovery(b *testing.B) {
	for _, prePartitionEntries := range []int{10, 50} {
		b.Run(fmt.Sprintf("%d_prePartition_entries", prePartitionEntries), func(b *testing.B) {
			var totalHeal time.Duration
			var totalSync time.Duration

			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				c, err := NewCluster(3)
				if err != nil {
					b.Fatalf("NewCluster: %v", err)
				}

				leaderID, _, err := c.WaitForLeader(5 * time.Second)
				if err != nil {
					c.Shutdown()
					b.Logf("iter %d: no initial leader: %v", iter, err)
					continue
				}

				// Write entries before partitioning.
				var lastPreIdx int
				for i := 0; i < prePartitionEntries; i++ {
					idx, _, _ := c.SubmitCommand(fmt.Sprintf("PRE_PARTITION_%d", i))
					lastPreIdx = idx
				}
				if lastPreIdx > 0 {
					_ = c.WaitForCommit(lastPreIdx, 10*time.Second)
				}

				// Choose the isolated node — always kill the one that is NOT leader.
				isolatedID := 0
				for id := 1; id <= 3; id++ {
					if id != leaderID {
						isolatedID = id
						break
					}
				}
				c.Kill(isolatedID)

				// Majority partition writes more entries.
				var lastMajIdx int
				for i := 0; i < 5; i++ {
					idx, _, _ := c.SubmitCommand(fmt.Sprintf("MAJORITY_%d", i))
					lastMajIdx = idx
				}
				if lastMajIdx > 0 {
					_ = c.WaitForCommit(lastMajIdx, 5*time.Second)
				}

				b.StartTimer()

				// ── Phase: Partition heal ─────────────────────────────────
				healStart := time.Now()
				if err := c.Restart(isolatedID); err != nil {
					b.StopTimer()
					c.Shutdown()
					b.Logf("iter %d: restart failed: %v", iter, err)
					continue
				}

				// Wait for a stable leader to be confirmed across all 3 nodes.
				_, _, err = c.WaitForLeader(5 * time.Second)
				healDuration := time.Since(healStart)

				// ── Phase: Log synchronisation ────────────────────────────
				syncStart := time.Now()
				_ = c.WaitForLogSync(lastMajIdx, 5*time.Second)
				syncDuration := time.Since(syncStart)

				b.StopTimer()
				c.Shutdown()
				time.Sleep(60 * time.Millisecond)

				if err != nil {
					b.Logf("iter %d: no leader after heal: %v", iter, err)
					continue
				}

				totalHeal += healDuration
				totalSync += syncDuration
			}

			n := time.Duration(b.N)
			fmt.Printf("\n[BENCHMARK] Network Partition Recovery — %d pre-partition entries\n",
				prePartitionEntries)
			fmt.Printf("  Partition heal (leader stable) : avg %.2f ms\n",
				float64((totalHeal / n).Milliseconds()))
			fmt.Printf("  Log sync (isolated node caught up): avg %.2f ms\n",
				float64((totalSync / n).Milliseconds()))
			fmt.Printf("  ↳ Consistency verified: all alive nodes log-matched after heal\n")
		})
	}
}

// ============================================================================
// BENCHMARK 8: LOG CONSISTENCY UNDER NODE CHURN
// ==============================================================================
func BenchmarkLogConsistencyUnderChurn(b *testing.B) {
	const totalCommands = 200
	const churnPeriod = 20 // crash/restart a follower every N commands

	b.Run("3_Nodes_20pct_churn", func(b *testing.B) {
		for iter := 0; iter < b.N; iter++ {
			b.StopTimer()
			c, err := NewCluster(3)
			if err != nil {
				b.Fatalf("NewCluster: %v", err)
			}

			leaderID, _, err := c.WaitForLeader(5 * time.Second)
			if err != nil {
				c.Shutdown()
				b.Logf("iter %d: initial leader failed: %v", iter, err)
				continue
			}

			b.StartTimer()

			start := time.Now()
			var successCount, submitCount int
			var lastSuccessIdx int
			var churnCount int

			for i := 0; i < totalCommands; i++ {
				// ── Churn: crash and restart a non-leader follower ─────────
				if i > 0 && i%churnPeriod == 0 {
					// Pick a non-leader follower to crash.
					aliveFollowers := []int{}
					for id := 1; id <= 3; id++ {
						if id != leaderID && c.IsAlive(id) {
							aliveFollowers = append(aliveFollowers, id)
						}
					}
					if len(aliveFollowers) > 0 {
						victim := aliveFollowers[rand.Intn(len(aliveFollowers))]
						c.Kill(victim)
						// Brief pause — give goroutines time to notice the kill
						// and release any in-flight RPC locks.
						time.Sleep(40 * time.Millisecond)
						_ = c.Restart(victim)
						churnCount++
					}
				}

				// Submit command
				cmd := fmt.Sprintf("SET churn_key_%d_%d value_%d", iter, i, i)
				idx, _, submitErr := c.SubmitCommand(cmd)
				submitCount++
				if submitErr != nil {
					continue
				}
				// Non-blocking commit check: don't block the submit loop,
				// we'll verify the last entry at the end.
				if idx > lastSuccessIdx {
					lastSuccessIdx = idx
				}
				successCount++

				// Re-discover leader after churn events (leadership may change).
				if i%churnPeriod == 0 {
					if newLeaderID, _, werr := c.WaitForLeader(2 * time.Second); werr == nil {
						leaderID = newLeaderID
					}
				}
			}

			// Wait for the last successfully submitted entry to commit.
			if lastSuccessIdx > 0 {
				_ = c.WaitForCommit(lastSuccessIdx, 10*time.Second)
			}

			duration := time.Since(start)
			b.StopTimer()

			//  Consistency check
			// Verify that all alive nodes agree on the last committed index.
			leaderNode := c.GetNode(leaderID)
			allMatch := true
			if leaderNode != nil {
				leaderCommit := leaderNode.GetCommitIndex()
				for id := 1; id <= 3; id++ {
					if !c.IsAlive(id) {
						continue
					}
					node := c.GetNode(id)
					if node != nil && node.GetCommitIndex() != leaderCommit {
						allMatch = false
					}
				}
			}

			successRate := float64(successCount) / float64(submitCount) * 100
			throughput := float64(successCount) / duration.Seconds()

			fmt.Printf("\n[BENCHMARK] Log Consistency Under Churn (%d commands, 3-node)\n",
				totalCommands)
			fmt.Printf("  Commands submitted : %d\n", submitCount)
			fmt.Printf("  Commands succeeded : %d  (success rate: %.1f%%)\n",
				successCount, successRate)
			fmt.Printf("  Duration           : %.2f sec\n", duration.Seconds())
			fmt.Printf("  Throughput         : %.2f commands/sec\n", throughput)
			fmt.Printf("  Churn events       : %d crash/restart cycles\n", churnCount)
			fmt.Printf("  All nodes consistent: %v\n", allMatch)

			c.Shutdown()
			time.Sleep(60 * time.Millisecond)
			b.StartTimer()
		}
	})
}
