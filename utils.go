// utils.go — MiniRaft Shared Utilities
//
// Small helpers used across multiple files. Kept here to avoid scattering
// utility logic throughout the codebase.

package main

import (
	"math/rand"
	"time"
)

// randomElectionTimeout returns a random duration between ElectionTimeoutMin
// and ElectionTimeoutMax. Each call to this function produces a different
// value, which is what gives each node a different window before it decides
// the leader has crashed and starts a campaign.
//
// Concurrency note: rand.Int63n uses a package-level source that is
// goroutine-safe in Go 1.20+ (global rand is automatically locked).
// In earlier versions you would need a per-node *rand.Rand; since we target
// Go 1.22+ the global rand is fine here.
func randomElectionTimeout() time.Duration {
	rangeMs := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	jitterMs := rand.Int63n(rangeMs / int64(time.Millisecond))
	return ElectionTimeoutMin + time.Duration(jitterMs)*time.Millisecond
}

// minInt returns the smaller of two ints. Used in AppendEntries to clamp
// leaderCommit values.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// maxInt returns the larger of two ints. Used for commitIndex calculations.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
