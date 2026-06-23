import re

with open('rpc.go', 'r') as f:
    content = f.read()

# Replace Case B logic in AppendEntries
case_b_old = """	// Case B: Entry at PrevLogIndex has the wrong term.
	// Note: PrevLogIndex==0 is the sentinel entry (always term=0), which
	// always matches PrevLogTerm=0. We skip this check for index 0.
	if args.PrevLogIndex > 0 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		reply.Success = false
		reply.ConflictTerm = conflictTerm

		// Find the FIRST index with conflictTerm. This allows the leader to
		// skip the entire conflicting term in one nextIndex decrement, rather
		// than decrementing one position at a time.
		//
		// Example: our log has 20 entries all with term=2.
		// Leader sends prevLogTerm=3 (doesn't match).
		// Without optimization: leader decrements nextIndex 20 times.
		// With optimization: ConflictIndex=1 → leader skips all 20 at once.
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 1 && n.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex

		n.logfLocked("REJECT AppendEntries: term mismatch at log[%d] "+
			"(expected term=%d, found term=%d) → conflictTerm=%d, conflictIndex=%d",
			args.PrevLogIndex, args.PrevLogTerm, conflictTerm,
			reply.ConflictTerm, reply.ConflictIndex)
		n.mu.Unlock()
		if stateChanged {
			n.persist()
		}
		n.resetElectionTimer()
		return nil
	}"""

case_b_new = """	offset := n.log[0].Index

	// Case B: Entry at PrevLogIndex has the wrong term.
	if args.PrevLogIndex < offset {
		reply.Success = false
		reply.ConflictIndex = offset + 1
		reply.ConflictTerm = 0
		n.mu.Unlock()
		return nil
	}

	if args.PrevLogIndex > offset && n.log[args.PrevLogIndex-offset].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex-offset].Term
		reply.Success = false
		reply.ConflictTerm = conflictTerm

		conflictIndex := args.PrevLogIndex
		for conflictIndex > offset+1 && n.log[conflictIndex-1-offset].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex

		n.logfLocked("REJECT AppendEntries: term mismatch at log[%d] "+
			"(expected term=%d, found term=%d) → conflictTerm=%d, conflictIndex=%d",
			args.PrevLogIndex, args.PrevLogTerm, conflictTerm,
			reply.ConflictTerm, reply.ConflictIndex)
		n.mu.Unlock()
		if stateChanged {
			n.persist()
		}
		n.resetElectionTimer()
		return nil
	} else if args.PrevLogIndex == offset && args.PrevLogIndex != 0 {
		if n.log[0].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictIndex = offset
			reply.ConflictTerm = 0
			n.mu.Unlock()
			return nil
		}
	}"""

content = content.replace(case_b_old, case_b_new)

# Replace Rule 3 & 4 truncation logic
trunc_old = """	for i, entry := range args.Entries {
		logIdx := args.PrevLogIndex + 1 + i

		if logIdx < len(n.log) {
			if n.log[logIdx].Term == entry.Term {
				// Same (index, term) → same entry (Log Matching Property).
				// Skip — do not overwrite, do not truncate. This makes the
				// handler idempotent: replaying the same AppendEntries is safe.
				continue
			}
			// Different term → genuine conflict. Truncate from logIdx
			// and append the rest of the leader's entries at once.
			n.logfLocked("TRUNCATE log[%d:] — conflict: our term=%d, leader term=%d",
				logIdx, n.log[logIdx].Term, entry.Term)
			n.log = n.log[:logIdx]
			n.log = append(n.log, args.Entries[i:]...)
			stateChanged = true
			break
		}

		// logIdx is past our log's current end — append remaining entries.
		n.log = append(n.log, args.Entries[i:]...)
		stateChanged = true
		break
	}"""

trunc_new = """	for i, entry := range args.Entries {
		logIdx := args.PrevLogIndex + 1 + i

		if logIdx < len(n.log)+offset {
			if n.log[logIdx-offset].Term == entry.Term {
				continue
			}
			n.logfLocked("TRUNCATE log[%d:] — conflict: our term=%d, leader term=%d",
				logIdx, n.log[logIdx-offset].Term, entry.Term)
			n.log = n.log[:logIdx-offset]
			n.log = append(n.log, args.Entries[i:]...)
			stateChanged = true
			break
		}

		n.log = append(n.log, args.Entries[i:]...)
		stateChanged = true
		break
	}"""

content = content.replace(trunc_old, trunc_new)

# Replace Leader sendAppendEntries loop slice
send_old = """		n.mu.RLock()
		if n.state != Leader {
			n.mu.RUnlock()
			return
		}
		nextIdx := n.nextIndex[peerID]
		prevLogIndex := nextIdx - 1
		prevLogTerm := n.getLogTerm(prevLogIndex)

		// Create a deep copy of the entries to send.
		// We copy to avoid holding a reference to n.log while the lock is released.
		// If we sent n.log[nextIdx:] directly, the RPC layer would hold a
		// reference to n.log[nextIdx:] and another goroutine appended to n.log,
		// it could cause a race condition.
		var entries []LogEntry
		if nextIdx <= n.lastLogIndex() {
			raw := n.log[nextIdx:]
			entries = make([]LogEntry, len(raw))
			copy(entries, raw)
		}
		
		leaderCommit := n.commitIndex
		n.mu.RUnlock()"""

send_new = """		n.mu.RLock()
		if n.state != Leader {
			n.mu.RUnlock()
			return
		}
		nextIdx := n.nextIndex[peerID]
		offset := n.log[0].Index

		if nextIdx <= offset {
			// Peer is too far behind; we must send a snapshot instead of AppendEntries.
			// Build InstallSnapshotArgs and release lock.
			args := &InstallSnapshotArgs{
				Term:              n.currentTerm,
				LeaderID:          n.id,
				LastIncludedIndex: offset,
				LastIncludedTerm:  n.log[0].Term,
			}
			
			// We need to read the snapshot file from disk without holding the lock.
			n.mu.RUnlock()
			
			snapData, err := os.ReadFile(fmt.Sprintf("data/node-%d.snapshot", n.id))
			if err != nil {
				n.logf("failed to read snapshot file for peer %d: %v", peerID, err)
				return
			}
			args.Data = snapData
			
			// We execute InstallSnapshot instead
			go n.sendInstallSnapshot(peerID, args)
			return
		}

		prevLogIndex := nextIdx - 1
		prevLogTerm := n.getLogTerm(prevLogIndex)

		var entries []LogEntry
		if nextIdx <= n.lastLogIndex() {
			raw := n.log[nextIdx-offset:]
			entries = make([]LogEntry, len(raw))
			copy(entries, raw)
		}
		
		leaderCommit := n.commitIndex
		n.mu.RUnlock()"""

content = content.replace(send_old, send_new)

# Append sendInstallSnapshot to rpc.go
install_snapshot = """
// sendInstallSnapshot sends an InstallSnapshot RPC to a peer.
func (n *Node) sendInstallSnapshot(peerID int, args *InstallSnapshotArgs) {
	client := n.getRPCClient(peerID)
	if client == nil {
		return
	}

	var reply InstallSnapshotReply
	err := client.Call("Node.InstallSnapshot", args, &reply)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != args.Term {
		return
	}

	if reply.Term > n.currentTerm {
		n.checkTerm(reply.Term)
		n.state = Follower
		n.persist()
		n.resetElectionTimer()
		return
	}

	// Update matchIndex and nextIndex
	if args.LastIncludedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = args.LastIncludedIndex
		n.nextIndex[peerID] = args.LastIncludedIndex + 1
	}
}
"""

if "func (n *Node) sendInstallSnapshot" not in content:
    content += install_snapshot

# Replace conflict index tracking in handleAppendEntriesReply
conflict_old = """					for i := n.lastLogIndex(); i >= 1; i-- {
						if n.log[i].Term == reply.ConflictTerm {
							n.nextIndex[peerID] = i + 1
							found = true
							break
						}
					}"""

conflict_new = """					offset := n.log[0].Index
					for i := n.lastLogIndex(); i > offset; i-- {
						if n.log[i-offset].Term == reply.ConflictTerm {
							n.nextIndex[peerID] = i + 1
							found = true
							break
						}
					}"""

content = content.replace(conflict_old, conflict_new)

# Replace commitIndex logic in Leader
commit_old = """		for N := n.lastLogIndex(); N > n.commitIndex; N-- {
			// Rule 2: n.log[N].Term == n.currentTerm
			// (A leader cannot commit entries from previous terms by counting replicas)
			if n.log[N].Term != n.currentTerm {
				continue
			}"""

commit_new = """		offset := n.log[0].Index
		for N := n.lastLogIndex(); N > n.commitIndex; N-- {
			if N <= offset {
				break
			}
			// Rule 2: n.log[N].Term == n.currentTerm
			if n.log[N-offset].Term != n.currentTerm {
				continue
			}"""

content = content.replace(commit_old, commit_new)

commit_log_old = """			n.logfLocked("🎯 COMMIT log[%d] cmd=%q term=%d — replicated on %d/%d nodes",
				N, n.log[N].Command, n.currentTerm,
				matchCount, len(n.peers)+1)"""

commit_log_new = """			n.logfLocked("🎯 COMMIT log[%d] cmd=%q term=%d — replicated on %d/%d nodes",
				N, n.log[N-offset].Command, n.currentTerm,
				matchCount, len(n.peers)+1)"""

content = content.replace(commit_log_old, commit_log_new)

with open('rpc.go', 'w') as f:
    f.write(content)

