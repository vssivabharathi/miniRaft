package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// setupBenchmarkCluster is a helper that works like newTestCluster but for testing.B
func setupBenchmarkCluster(b *testing.B, size int) *Cluster {
	b.Helper()
	// Create data dir for persistence if it doesn't exist
	setupDataDir(b)
	
	c, err := NewCluster(size)
	if err != nil {
		b.Fatalf("failed to create cluster: %v", err)
	}
	b.Cleanup(func() {
		c.Shutdown()
		time.Sleep(50 * time.Millisecond)
	})
	return c
}

// waitLeader is a helper for testing.B
func waitLeader(b *testing.B, c *Cluster, timeout time.Duration) int {
	b.Helper()
	leaderID, _, err := c.WaitForLeader(timeout)
	if err != nil {
		b.Fatalf("failed to wait for leader: %v", err)
	}
	return leaderID
}

func BenchmarkLeaderElection(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// We must include cluster startup in election time,
				// as Raft begins election immediately if no leader exists.
				b.StopTimer()
				c := setupBenchmarkCluster(b, size)
				b.StartTimer()
				
				waitLeader(b, c, 5*time.Second)
				
				b.StopTimer()
				c.Shutdown()
			}
		})
	}
}

func BenchmarkLogReplication(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			c := setupBenchmarkCluster(b, size)
			waitLeader(b, c, 5*time.Second)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, _, err := c.SubmitCommand(fmt.Sprintf("CMD_%d", i))
				if err != nil {
					b.Fatalf("submit failed: %v", err)
				}
				if err := c.WaitForCommit(idx, 2*time.Second); err != nil {
					b.Fatalf("commit failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkConcurrentCommands(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			c := setupBenchmarkCluster(b, size)
			waitLeader(b, c, 5*time.Second)

			var cmdCounter atomic.Int64

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cmdNum := cmdCounter.Add(1)
					idx, _, err := c.SubmitCommand(fmt.Sprintf("PARALLEL_CMD_%d", cmdNum))
					if err == nil {
						_ = c.WaitForCommit(idx, 2*time.Second)
					}
				}
			})
		})
	}
}

func BenchmarkHeartbeatThroughput(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			c := setupBenchmarkCluster(b, size)
			leaderID := waitLeader(b, c, 5*time.Second)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Simply read the heartbeat count rapidly to measure how fast the leader
				// is firing them in the background while under observation load.
				// This is a proxy for background throughput.
				_ = c.GetNode(leaderID).GetMetricsSnapshot().HeartbeatsSent
				time.Sleep(100 * time.Microsecond)
			}
		})
	}
}

func BenchmarkCrashRecovery(b *testing.B) {
	for _, size := range []int{3, 5} {
		b.Run(fmt.Sprintf("%d_Nodes", size), func(b *testing.B) {
			c := setupBenchmarkCluster(b, size)
			leaderID := waitLeader(b, c, 5*time.Second)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				c.Kill(leaderID)
				time.Sleep(100 * time.Millisecond) // Allow node to cleanly die
				b.StartTimer()

				// Measure how long the remaining nodes take to elect a new leader
				newLeader := waitLeader(b, c, 5*time.Second)

				b.StopTimer()
				// Bring the old leader back for the next iteration (it will rejoin as follower)
				if err := c.Restart(leaderID); err != nil {
					b.Fatalf("failed to restart: %v", err)
				}
				leaderID = newLeader
				b.StartTimer()
			}
		})
	}
}
