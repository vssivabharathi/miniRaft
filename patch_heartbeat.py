import re

with open('heartbeat.go', 'r') as f:
    content = f.read()

# Replace sendHeartbeatToOne logic
send_old = """	prevLogIndex := n.nextIndex[peerID] - 1
	prevLogTerm := n.getLogTerm(prevLogIndex)

	var entries []LogEntry
	if n.nextIndex[peerID] <= n.lastLogIndex() {
		// Follower is behind — include entries in this heartbeat round.
		raw := n.log[n.nextIndex[peerID]:]
		entries = make([]LogEntry, len(raw))
		copy(entries, raw)
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,"""

send_new = """	offset := n.log[0].Index
	if n.nextIndex[peerID] <= offset {
		// Needs snapshot
		args := &InstallSnapshotArgs{
			Term:              n.currentTerm,
			LeaderID:          n.id,
			LastIncludedIndex: offset,
			LastIncludedTerm:  n.log[0].Term,
		}
		n.mu.RUnlock()
		
		snapData, err := os.ReadFile(fmt.Sprintf("data/node-%d.snapshot", n.id))
		if err != nil {
			n.logf("failed to read snapshot file for peer %d: %v", peerID, err)
			return
		}
		args.Data = snapData
		
		go n.sendInstallSnapshot(peerID, args)
		return
	}

	prevLogIndex := n.nextIndex[peerID] - 1
	prevLogTerm := n.getLogTerm(prevLogIndex)

	var entries []LogEntry
	if n.nextIndex[peerID] <= n.lastLogIndex() {
		// Follower is behind — include entries in this heartbeat round.
		raw := n.log[n.nextIndex[peerID]-offset:]
		entries = make([]LogEntry, len(raw))
		copy(entries, raw)
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,"""

content = content.replace(send_old, send_new)

# In handle heartbeat reply, if conflict, we backtrack.
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

with open('heartbeat.go', 'w') as f:
    f.write(content)

