// election.go — MiniRaft Leader Election
//
// This file implements the complete Raft leader election protocol as described
// in §5.2 and §5.4 of the Raft paper (Ongaro & Ousterhout, 2014).
//
// Raft solves leader election with three key properties:
//
//   Election Safety   — At most one leader can be elected in any given term.
//                       (§5.2) Enforced by the one-vote-per-term rule.
//
//   Leader Completeness — A newly elected leader has all committed entries.
//                        (§5.4) Enforced by the log up-to-date check in
//                        RequestVote.
//
//   Leader Append-Only — A leader never overwrites or deletes entries in its
//                        log; it only appends new ones. (§5.3)
//
// Concurrency model in this file:
//
//   The election protocol involves multiple concurrent goroutines that share
//   the Node's mutable state. The invariants we maintain are:
//
//     1. State (currentTerm, votedFor, state) is read and written only while
//        holding n.mu (write lock for mutations, read lock for reads).
//
//     2. Network I/O (RPC calls) is NEVER performed while holding n.mu.
//        Before any RPC: snapshot required state under lock, release lock.
//        After  any RPC: reacquire lock, validate reply is not stale.
//
//     3. The votes counter is a plain int accessed only under n.mu.
//        No additional synchronization primitive is needed for it.
//
//     4. Stale-reply prevention: every reply goroutine verifies that
//        (n.state == Candidate && n.currentTerm == termAtStart) before
//        processing a vote. Replies from previous election epochs are
//        silently discarded.
//
//     5. Double-promotion prevention: becomeLeader() sets n.state = Leader.
//        Subsequent goroutines that reach quorum see n.state != Candidate
//        and exit immediately — no sync.Once or atomic needed.

package main

import "fmt"

// ---------------------------------------------------------------------------
// RPC argument / reply types
// ---------------------------------------------------------------------------

// RequestVoteArgs contains the arguments sent by a Candidate when requesting
// a vote from another node. Every field corresponds to a field in the
// RequestVote RPC specification in Figure 2 of the Raft paper (§5.2).
//
// The LastLogIndex and LastLogTerm fields implement the Election Restriction
// (§5.4): a candidate can only receive a vote if its log is at least as
// up-to-date as the voter's log. This prevents nodes with stale logs from
// being elected and potentially overwriting committed entries.
type RequestVoteArgs struct {
	// Term is the candidate's current term. If the recipient's currentTerm is
	// greater, it will reject the vote and the candidate will step down.
	Term int

	// CandidateID is the unique ID of the node running for leader.
	// Recipients use this to record whom they voted for (votedFor field).
	CandidateID int

	// LastLogIndex is the index of the candidate's last log entry.
	// Used for the log up-to-date comparison in the Election Restriction.
	LastLogIndex int

	// LastLogTerm is the term of the candidate's last log entry.
	// Compared with the voter's lastLogTerm first; index is a tiebreaker.
	LastLogTerm int
}

// RequestVoteReply contains the response to a RequestVote RPC.
// Per Figure 2 of the Raft paper (§5.2).
type RequestVoteReply struct {
	// Term is the voter's currentTerm. The candidate uses this to update itself
	// if it discovers a higher term — it will step down to Follower immediately.
	Term int

	// VoteGranted is true if the voter granted its vote to the candidate.
	// False means the candidate did not receive this vote (wrong term, already
	// voted, or stale log).
	VoteGranted bool
}

// ---------------------------------------------------------------------------
// RequestVote — the RPC handler (run on the receiving node)
// ---------------------------------------------------------------------------

// RequestVote is the net/rpc handler called by peer nodes campaigning for
// leadership. It implements the RequestVote RPC receiver logic described in
// Figure 2 of the Raft paper (§5.2, §5.4).
//
// Raft paper reference: §5.2 (Leader Election), §5.4 (Safety — Election Restriction)
//
// Grant conditions (ALL must be true to grant the vote):
//  1. args.Term >= n.currentTerm                 (candidate is not from old epoch)
//  2. n.votedFor == -1 || == args.CandidateID    (haven't voted, or idempotent retry)
//  3. candidate's log is at least as up-to-date  (Election Restriction — §5.4)
//
// Election Restriction (§5.4) explained:
//
//	"Raft determines which of two logs is more up-to-date by comparing the
//	 index and term of the last entries in the logs. If the logs have last
//	 entries with different terms, then the log with the later term is more
//	 up-to-date. If the logs end with the same term, then whichever log is
//	 longer is more up-to-date."
//
//	Without this restriction, a node that missed committed entries could be
//	elected leader and overwrite them, violating the safety guarantee.
//
// Safety guarantees enforced:
//   - One vote per term: votedFor tracks the single grantee per term.
//   - No stale leader: only candidates with up-to-date logs win.
//   - Higher-term step-down: the universal Raft rule is applied first.
//
// Timer reset policy:
//   - Vote GRANTED  → reset election timer.
//     We just vouched for this candidate. Give them time to win and establish
//     leadership before we start our own campaign.
//   - Vote DENIED   → do NOT reset timer.
//     No evidence of a legitimate authority. We may need to start our own
//     election soon.
//
// Common bugs:
//   - Missing the log up-to-date check (violates safety — stale leader elected)
//   - Resetting timer on denied votes (liveness issue — may never elect leader)
//   - Not handling votedFor == args.CandidateID (breaks idempotency — duplicate
//     RPCs would cause the node to deny a vote it already granted)
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Dead-node guard: reject any RPC if this node has been killed.
	if n.isDead() {
		return fmt.Errorf("node %d is dead", n.id)
	}

	n.metrics.RPCReceived.Add(1)

	n.mu.Lock()

	// -------------------------------------------------------------------
	// Rule 1: Reject votes from candidates in stale terms.
	// §5.1: "If a server receives a request with a stale term number,
	//        it rejects the request."
	// Inform the stale candidate of the current term so it steps down.
	// -------------------------------------------------------------------
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		n.logfLocked("DENY vote → Node %d (their term %d < our term %d — stale candidate)",
			args.CandidateID, args.Term, n.currentTerm)
		n.mu.Unlock()
		return nil
	}

	// -------------------------------------------------------------------
	// Rule 2: Higher term observation — step down immediately.
	// §5.1: "If RPC request or response contains term T > currentTerm:
	//        set currentTerm = T, convert to follower."
	//
	// After checkTerm:
	//   - n.currentTerm == args.Term
	//   - n.state       == Follower
	//   - n.votedFor    == -1  (new term, clean vote slate)
	//
	// Note: we do not return here. After stepping down, we still evaluate
	// whether to grant the vote for this new term.
	// -------------------------------------------------------------------
	steppedDown := n.checkTerm(args.Term)

	// -------------------------------------------------------------------
	// Rule 3: Evaluate vote grant conditions.
	// -------------------------------------------------------------------

	// Condition A — have we already voted this term?
	// votedFor == -1          → not yet voted → eligible
	// votedFor == CandidateID → idempotent retry → eligible (safe to re-grant)
	// votedFor == other ID    → already voted for someone else → deny
	canVote := n.votedFor == -1 || n.votedFor == args.CandidateID

	// Condition B — Election Restriction (§5.4.1).
	// The candidate's log must be at least as up-to-date as ours.
	// isLogUpToDate compares (lastLogTerm, lastLogIndex) pairs.
	logUpToDate := n.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)

	grantVote := canVote && logUpToDate

	reply.Term = n.currentTerm // always reflect our current (possibly updated) term

	if grantVote {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.logfLocked("GRANT vote → Node %d (term=%d, candidateLog=[%d,%d], ourLog=[%d,%d], steppedDown=%v)",
			args.CandidateID, args.Term,
			args.LastLogIndex, args.LastLogTerm,
			n.lastLogIndex(), n.lastLogTerm(),
			steppedDown)
	} else {
		reply.VoteGranted = false
		n.logfLocked("DENY vote → Node %d (canVote=%v, logUpToDate=%v, votedFor=%d)",
			args.CandidateID, canVote, logUpToDate, n.votedFor)
	}

	n.mu.Unlock()

	if steppedDown || grantVote {
		n.persist()
	}

	// -------------------------------------------------------------------
	// Timer reset — AFTER releasing the lock.
	// resetElectionTimer() accesses n.electionTimer, which is safe to
	// call without n.mu (timer operations are internally synchronized).
	// We must NOT hold n.mu here — timer operations can block briefly.
	// -------------------------------------------------------------------
	if grantVote {
		// Reset timer so we don't start a competing election while the
		// candidate we just voted for is establishing leadership.
		n.resetElectionTimer()
	}

	return nil
}

// ---------------------------------------------------------------------------
// startElection — the candidate's campaign logic
// ---------------------------------------------------------------------------

// startElection runs a complete Raft election cycle. It is invoked by
// runElectionTicker when the election timer fires.
//
// Raft paper reference: §5.2 (Leader Election), Figure 2 (Rules for Servers)
//
// The function implements the "Candidates" rules:
//   - On conversion to candidate, start election:
//   - Increment currentTerm
//   - Vote for self
//   - Reset election timer  (done by the ticker that calls us)
//   - Send RequestVote RPCs to all other servers
//
// Concurrency design:
//
//	Phase 1 (under lock):
//	  - Transition to Candidate: becomeCandidate() atomically increments
//	    term, sets votedFor = self, sets state = Candidate.
//	  - Snapshot all state needed for the RPC args (term, last log index/term).
//	  - Release the lock.
//
//	Phase 2 (no lock):
//	  - Build RequestVoteArgs from the snapshot.
//	  - Launch one goroutine per peer to send the RPC concurrently.
//	  - The goroutines share a single `votes` counter via closure.
//
//	Phase 3 (each goroutine, under lock):
//	  - Reacquire n.mu before processing the reply.
//	  - Apply two stale-reply guards (state check, term check).
//	  - Apply the higher-term step-down rule.
//	  - Increment votes and check for majority.
//	  - On majority: call becomeLeader() under the lock.
//
// Split-vote handling:
//
//	If the election timeout fires again before we reach majority (split vote),
//	runElectionTicker starts a new election with an incremented term.
//	The randomized timeout makes it statistically unlikely for split votes to
//	persist more than 1-2 rounds.
//
// Common bugs and mitigations:
//
//	Bug: Holding n.mu during RPC → deadlock.
//	  Fix: snapshot args under lock, release before calling sendRequestVote.
//
//	Bug: Processing stale reply from old election.
//	  Fix: guard on n.currentTerm == termAtStart (closure captures term).
//
//	Bug: Double-promotion to Leader.
//	  Fix: guard on n.state == Candidate; becomeLeader sets state=Leader;
//	       subsequent goroutines see state≠Candidate and exit.
//
//	Bug: Loop variable capture in goroutine closure.
//	  Fix: pass peerID as argument to the goroutine func (pid int).
func (n *Node) startElection() {
	// =========================================================================
	// PHASE 1: Transition to Candidate (under lock)
	// =========================================================================
	n.mu.Lock()

	// Safety guard: only Followers and Candidates may start elections.
	// A Leader never campaigns — it already won.
	// A Candidate can start a new election (split vote re-try, new term).
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	// becomeCandidate() atomically:
	//   n.currentTerm++
	//   n.votedFor    = n.id       (self-vote — first vote of the election)
	//   n.state       = Candidate
	n.becomeCandidate()

	// Snapshot the values we need for RequestVoteArgs.
	// These are read under the lock and used after it is released.
	// We must not re-read n.currentTerm, n.lastLogIndex(), etc. after
	// releasing the lock, as they could be mutated by a concurrent RPC.
	termAtStart := n.currentTerm // the term for THIS election epoch
	lastLogIdx := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()

	n.mu.Unlock()

	n.persist()

	n.logf("⚡ ELECTION START (term=%d, lastLogIndex=%d, lastLogTerm=%d)",
		termAtStart, lastLogIdx, lastLogTerm)

	// =========================================================================
	// PHASE 2: Prepare and fan out RequestVote RPCs (no lock held)
	// =========================================================================

	// Build the args struct from the snapshot. Immutable from here on —
	// all goroutines share the same pointer safely because they never write to it.
	args := &RequestVoteArgs{
		Term:         termAtStart,
		CandidateID:  n.id,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}

	// votes counts how many nodes have voted for us, including our self-vote.
	// Starts at 1 because becomeCandidate() sets votedFor = n.id.
	//
	// CONCURRENCY NOTE: votes is accessed ONLY while holding n.mu.
	// All reply goroutines acquire n.mu before reading or writing votes.
	// Therefore, votes does not need to be an atomic or a channel — the
	// existing node mutex provides sufficient protection.
	votes := 1

	for peerID := range n.peers {
		// ⚠️  LOOP VARIABLE CAPTURE: pass peerID as a function argument
		// (not captured by reference from the loop variable). Without this,
		// all goroutines would close over the same loop variable and see
		// its final value after the loop completes — a classic Go gotcha.
		go func(pid int) {
			reply := &RequestVoteReply{}

			// =================================================================
			// RPC CALL — no lock held during network I/O
			// =================================================================
			// sendRequestVote handles connection management (lazy dial, retry on
			// broken connection). It may block for up to the RPC timeout.
			// This is intentional — we do NOT want to hold n.mu while blocked.
			ok := n.sendRequestVote(pid, args, reply)
			if !ok {
				// Peer is unreachable (crashed, not yet started, network issue).
				// This is not a fatal error. We can still win the election if
				// other peers grant us their votes.
				n.logf("no reply from Node %d (RequestVote term=%d) — peer may be down",
					pid, termAtStart)
				return
			}

			// =================================================================
			// PHASE 3: Process the reply (under lock)
			// =================================================================
			n.mu.Lock()
			steppedDown := false
			becameLeader := false
			defer func() {
				n.mu.Unlock()
				if steppedDown || becameLeader {
					n.persist()
				}
			}()

			// -----------------------------------------------------------------
			// Stale-reply Guard A: State check.
			// We may have stepped down (saw higher term) or already won the
			// election between the time we sent the RPC and now.
			// If we're no longer a Candidate, this reply is irrelevant.
			// -----------------------------------------------------------------
			if n.state != Candidate {
				n.logfLocked("discard vote reply from Node %d — no longer Candidate (state=%s, term=%d)",
					pid, n.state, n.currentTerm)
				return
			}

			// -----------------------------------------------------------------
			// Stale-reply Guard B: Term check.
			// Our term may have advanced since we sent this RequestVote.
			// A reply for term T cannot legitimately affect an election in term T+N.
			// termAtStart is captured in the closure and never changes.
			// -----------------------------------------------------------------
			if n.currentTerm != termAtStart {
				n.logfLocked("discard vote reply from Node %d — term mismatch (sent term=%d, current term=%d)",
					pid, termAtStart, n.currentTerm)
				return
			}

			// -----------------------------------------------------------------
			// Higher-term step-down.
			// Even a rejected vote carries the voter's currentTerm. If that term
			// is higher than ours, it means a new election epoch has started
			// (possibly by another node) and we must immediately revert to Follower.
			// This covers the case where the network recovers a partitioned leader.
			// -----------------------------------------------------------------
			if reply.Term > n.currentTerm {
				n.logfLocked("stepping down — Node %d replied with higher term %d (our term=%d)",
					pid, reply.Term, n.currentTerm)
				n.checkTerm(reply.Term)
				steppedDown = true
				return
			}

			// -----------------------------------------------------------------
			// Vote counting and majority detection.
			// -----------------------------------------------------------------
			if reply.VoteGranted {
				votes++
				quorum := n.quorumSize()
				n.logfLocked("✓ vote received from Node %d — total %d/%d (need %d for quorum)",
					pid, votes, len(n.peers)+1, quorum)

				if votes >= quorum {
					// =======================================================
					// MAJORITY REACHED — BECOME LEADER
					// =======================================================
					// This block is protected by n.mu. becomeLeader() sets
					// n.state = Leader. Any other goroutine reaching this point
					// will fail Guard A (n.state != Candidate) and exit.
					// This is the double-promotion prevention mechanism.
					//
					// becomeLeader() initializes:
					//   nextIndex[peer]  = lastLogIndex + 1  (optimistic)
					//   matchIndex[peer] = 0                  (conservative)
					//
					// Phase 4 note: sendHeartbeats() is defined in heartbeat.go.
					// The leader immediately begins sending heartbeats to
					// establish authority and suppress follower election timers.
					n.becomeLeader()
					becameLeader = true

					// Start heartbeat loop in a separate goroutine.
					// This goroutine is defined in heartbeat.go (Phase 4).
					// It runs until the node is killed or steps down.
					go n.sendHeartbeats()
				}
			} else {
				n.logfLocked("✗ vote denied by Node %d (their term=%d)",
					pid, reply.Term)
			}
		}(peerID)
	}
}

// ---------------------------------------------------------------------------
// sendRequestVote — thin RPC wrapper
// ---------------------------------------------------------------------------

// sendRequestVote sends a RequestVote RPC to peerID and writes the response
// into reply. Returns true on success, false if the peer is unreachable or
// the RPC fails.
//
// Responsibilities:
//   - Obtain the RPC client for peerID (lazy connect via getClient).
//   - Execute the blocking net/rpc call.
//   - On error: close the broken client (getClient will reconnect next time).
//
// Callers MUST NOT hold n.mu when calling this function.
// getClient() itself acquires and releases n.mu internally. Holding n.mu
// here while getClient() tries to acquire it would deadlock.
//
// Why a separate function instead of inlining the call?
//   - Keeps the goroutine body in startElection() focused on protocol logic.
//   - Makes it easy to inject a mock transport in tests (replace this function).
//   - Centralizes connection-error handling in one place.
func (n *Node) sendRequestVote(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	n.metrics.RPCSent.Add(1)

	client := n.getClient(peerID)
	if client == nil {
		// Peer is unreachable — connection could not be established.
		return false
	}

	// client.Call blocks until the RPC completes or the connection fails.
	// The net/rpc package handles TCP framing and encoding (gob by default).
	// The method name must match exactly: "ReceiverType.MethodName".
	err := client.Call("Node.RequestVote", args, reply)
	if err != nil {
		// Connection broken (peer crashed, network partition, etc.)
		// Close the client so getClient() creates a fresh connection next time.
		n.logf("RPC error on RequestVote to Node %d: %v", peerID, err)
		n.closeClient(peerID)
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// Election ticker goroutine
// ---------------------------------------------------------------------------

// runElectionTicker is the long-running background goroutine that drives the
// Raft election protocol. It runs from node startup until the node is killed.
//
// Raft paper reference: §5.2
//
//	"If a follower receives no communication over a period of time called the
//	 election timeout, then it assumes there is no viable leader and begins
//	 an election to choose a new leader."
//
// Behavior per iteration:
//  1. Block on n.electionTimer.C until the timer fires.
//  2. If node is dead, exit.
//  3. If node is Leader, skip election (Leaders don't campaign).
//  4. Otherwise (Follower or Candidate): start election in a new goroutine.
//  5. Reset the timer for the next round.
//
// Why a goroutine for startElection()?
//
//	startElection() dispatches N concurrent RPC goroutines and each waits for
//	a reply. Running startElection() synchronously would block the ticker loop
//	for the entire RPC timeout duration (potentially hundreds of milliseconds).
//	During that time, the timer would not be reset — if the election fails
//	(split vote), the node could not immediately start a new one.
//
//	By starting startElection() asynchronously, the ticker resets the timer
//	immediately and remains responsive. If the election succeeds, subsequent
//	timer fires are ignored (Leader branch). If it fails, the next fire starts
//	a new election.
//
// Why does a Leader NOT run an election?
//
//	A Leader already has majority authority for the current term. It maintains
//	that authority via heartbeats (see heartbeat.go). Starting an election
//	would wastefully increment the term and disrupt the cluster.
//	If the Leader later steps down (sees a higher term), it becomes a Follower
//	and this ticker drives its next election normally.
//
// Common bugs:
//   - Calling startElection() synchronously (ticker becomes unresponsive).
//   - Not checking isDead() → goroutine leak after node crash.
//   - Not resetting the timer in the Leader branch → timer never fires again
//     if the node later steps down (it would remain stuck, never electing).
func (n *Node) runElectionTicker() {
	// Capture the timer assigned to this specific goroutine's lifecycle.
	n.mu.RLock()
	myTimer := n.electionTimer
	n.mu.RUnlock()

	for {
		// ---------------------------------------------------------------
		// Shutdown check — exit cleanly before touching the timer channel.
		// ---------------------------------------------------------------
		if n.isDead() {
			n.logf("election ticker exiting — node killed")
			return
		}

		// ---------------------------------------------------------------
		// ---------------------------------------------------------------
		// Read the timer pointer under a read-lock to avoid a data race
		// with Revive() which sets n.electionTimer = nil.
		// If the timer was replaced (or is nil), this is a stale goroutine
		// from a previous lifecycle and should exit cleanly.
		// ---------------------------------------------------------------
		n.mu.RLock()
		currentTimer := n.electionTimer
		n.mu.RUnlock()

		if currentTimer != myTimer || currentTimer == nil {
			n.logf("election ticker exiting — timer cleared or replaced by Revive")
			return
		}

		<-currentTimer.C

		// Post-wakeup dead check — node may have been killed while sleeping.
		if n.isDead() {
			n.logf("election ticker exiting — node killed (post-wakeup)")
			return
		}

		// ---------------------------------------------------------------
		// Read current state under read-lock. A read-lock is sufficient
		// here because we only need a point-in-time snapshot for the
		// branch decision. We release immediately before doing any work.
		// ---------------------------------------------------------------
		n.mu.RLock()
		state := n.state
		n.mu.RUnlock()

		switch state {
		case Leader:
			// Leaders do not run elections.
			// We still reset the timer below so that if this node later
			// steps down to Follower, the timer fires at the correct time.
			n.logf("election timer fired — suppressed (we are Leader)")

		case Follower:
			// Follower timed out: no heartbeat received from leader.
			// Begin a campaign for the next term.
			n.logf("election timer fired — starting election (was Follower)")
			go n.startElection()

		case Candidate:
			// Previous election round did not complete in time (split vote
			// or lost messages). Start a new election with a higher term.
			// The randomized timeout in resetElectionTimer() makes it
			// statistically likely that one node times out before others
			// in the next round, breaking the split-vote cycle.
			n.logf("election timer fired — restarting election (was Candidate, split vote likely)")
			go n.startElection()
		}

		// Always reset the timer — with a fresh randomized duration.
		// This ensures the timer fires again for the next round.
		// Randomization is critical here: if all nodes reset to the same
		// duration, they would all time out simultaneously in the next
		// round, causing another split vote.
		n.resetElectionTimer()
	}
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

// StartElectionTicker initializes the election timer and starts the election
// ticker goroutine. Must be called exactly once per node before the node can
// participate in elections.
//
// Why separate from NewNode()?
//
//	Separating construction from activation is a key principle for testability.
//	Tests can construct a Node, inspect its initial state, manipulate fields,
//	and start the ticker at a deterministic point. If goroutines started
//	automatically in NewNode(), tests would have race conditions on startup.
//
//	This pattern matches how production systems like etcd's raft library
//	separate Node creation (raft.NewRawNode) from starting (node.run()).
//
// Ordering:
//
//	The timer is initialized BEFORE the goroutine starts. This guarantees
//	that n.electionTimer is non-nil when runElectionTicker() first reads it,
//	avoiding a nil pointer dereference if the goroutine is scheduled
//	immediately after go n.runElectionTicker() but before the timer is set.
func (n *Node) StartElectionTicker() {
	n.logf("starting election ticker (timeout=%v–%v, heartbeat=%v)",
		ElectionTimeoutMin, ElectionTimeoutMax, HeartbeatInterval)

	// Initialize the timer before the goroutine can reference it.
	n.resetElectionTimer()

	// The ticker goroutine runs for the lifetime of the node.
	go n.runElectionTicker()
}
