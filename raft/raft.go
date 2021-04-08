package raft

// Raft implementation.

// This file is divided into the following sections, each containing functions
// that implement the roles described in the Raft paper.

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must
// 1) call the AppendEntries RPC of its followers (either with empty messages or
// with entries to append) and 2) check whether the commit index has advanced.

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

import (
	// "raft/common" If want to use DieIfError.

	// "bytes" Part 4
	// "encoding/gob" Part 4
	"math/rand"
	"time"

	"github.com/golang/glog"
)

// Resets the election timer. Randomizes timeout to between 0.5 * election
// timeout and 1.5 * election timeout.
// REQUIRES r.mu
func (r *Raft) resetTimer() {
	to := int64(RaftElectionTimeout) / 2
	d := time.Duration(rand.Int63()%(2*to) + to)
	r.deadline = time.Now().Add(d)
}

// getStateLocked returns the current state while locked.
// REQUIRES r.mu
func (r *Raft) getStateLocked() (int, bool) {
	return r.currentTerm, r.state == leader
}

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Called once every n ms, where n is < election timeout. Checks whether the
// election has timed out. If so, starts an election. If not, returns
// immediately.
// EXCLUDES r.mu
func (r *Raft) electOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if time.Now().Before(r.deadline) {
		// Election deadline hasn't passed. Try later.
		// See if we can commit.
		go r.commit()
		return true
	}

	// Deadline has passed. Convert to candidate and start an election.
	glog.V(1).Infof("%d starting election for term %d", r.me, r.currentTerm)

	// TODO: Implement becoming a candidate (Rules for Servers: Candidates).

	// Send RequestVote RPCs to followers.
	r.sendBallots()

	return true
}

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must:
// call AppendEntries on its followers (either with empty messages or with
// entries to append) and check whether the commit index has advanced.

// Called once every n ms, where n is much less than election timeout. If the
// process is a leader, performs leadership duties.
// EXCLUDES r.mu
func (r *Raft) leadOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if r.state != leader {
		// Not leader, try later.
		return true
	}

	// Reset deadline timer.
	r.resetTimer()
	// Send heartbeat messages.
	r.sendAppendEntries()
	// Update commitIndex, if possible.
	r.updateCommitIndex()

	return true
}

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send RequestVote RPCs to all peers.
// REQUIRES r.mu
func (r *Raft) sendBallots() {
	for p := range r.peers {
		if p == r.me {
			continue
		}
		// TODO: Create RequestVoteArgs for peer p
		args := requestVoteArgs{}
		go r.sendBallot(p, args)
	}
}

// Send an individual ballot to a peer; upon completion, check if we are now the
// leader. If so, become leader.
// EXCLUDES r.mu
func (r *Raft) sendBallot(peer int, args requestVoteArgs) {
	// Call RequestVote on peer.
	var reply requestVoteReply
	if ok := r.callRequestVote(peer, args, &reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", peer)
		return
	}

	// TODO: Handle reply; implement Candidate rules for Servers.
	// After Part 1, this code should be relatively stable, presuming Part 1 is
	// implemented correctly. If accessing Raft state, make sure a Mutex is held.

	return
}

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Send AppendEntries RPCs to all followers.
// REQUIRES r.mu
func (r *Raft) sendAppendEntries() {
	for p := range r.peers {
		if p == r.me {
			continue
		}
		// TODO: Create AppendEntriesArgs for peer p.
		args := appendEntriesArgs{}
		go r.sendAppendEntry(p, args)
	}
}

// Send a single AppendEntries RPC to a follower. After it returns, update state
// based on the response.
// EXCLUDES r.mu
func (r *Raft) sendAppendEntry(peer int, args appendEntriesArgs) {
	// Call AppendEntries on peer.
	glog.V(6).Infof("%d calling ae on %d", r.me, peer)
	var reply appendEntriesReply
	if ok := r.callAppendEntries(peer, args, &reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", peer)
		return
	}

	// TODO: Handle reply; implement Leader rules for Servers.
	// Implement the leader portion of AppendEntries from the paper.

	// For part 1, you don't have to worry about properly updating nextIndex and
	// matchIndex.

	// For part 2, you will have to handle updating these values in response to
	// successful and failed RPCs.

	return
}

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// RequestVote RPC handler.
// EXCLUDES r.mu
func (r *Raft) RequestVote(args requestVoteArgs, reply *requestVoteReply) {
	// TODO: Implement the RequestVote receiver behavior from the paper.

	// For part 1, you don't have to worry about validating the "up to date"
	// property of logs.

	// For part 2, you will have to validate that the calling process is a
	// candidate for leader based on the length and terms of its logs.
	// 	 Section 5.4: "If the logs have last entries with different terms, then the
	// 	 log with the later term is more up-to-date. If the logs end with the same
	// 	 term, then whichever log is longer is more up-to-date."

	return
}

// AppendEntries RPC handler.
// EXCLUDES r.mu
func (r *Raft) AppendEntries(args appendEntriesArgs, reply *appendEntriesReply) {
	// TODO: Implement the AppendEntries receiver behavior from the paper.

	// For part 1, you do not have to handle appending log entries or detecting
	// term conflicts in the log. The deadline timer will need to be reset for
	// valid leader heartbeats.

	// For parts 2-4, you will have to handle log appends and conflict management.
	// You will also need to implement the optimization described in section 5.3
	// (also described in raft_api.go) in order for certain integration tests to
	// pass. If you don't implement it right away, you may need to modify unit
	// tests to ignore it (for now).

	return
}

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Update commitIndex for the leader based on the match indices of followers.
// REQUIRES r.mu
func (r *Raft) updateCommitIndex() {
	// TODO: Update commit index; implement Leaders part of Rules for Servers.

	// From the paper:
	// "If there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N"

	return
}

// Commit any outstanding committable indices.
// EXCLUDES r.mu
func (r *Raft) commit() {
	// TODO: Commit any entries between lastApplied and commitIndex.
	return
}

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Save persistent state so that it can be recovered from the persistence layer
// after process recovery.
// REQUIRES r.mu
func (r *Raft) persist() {
	if r.persister == nil {
		// Null persister; called in test.
		return
	}

	// TODO: Encode data and persist it using r.persister.WritePersistentState.

	return
}

// Called during recovery with previously-persisted state.
// REQUIRES r.mu
func (r *Raft) recoverState(data []byte) {
	if data == nil || len(data) < 1 { // Bootstrap.
		r.log = append(r.log, LogEntry{0, nil})
		r.persist()
		return
	}

	if r.persister == nil {
		// Null persister; called in test.
		return
	}

	// TODO: Recover from persisted data.

	return
}

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

// Call RequestVote on the given server. reply will be populated by the
// receiver server.
func (r *Raft) requestVote(server int, args requestVoteArgs, reply *requestVoteReply) {
	if ok := r.callRequestVote(server, args, reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", server)
		return
	}
}

// Call AppendEntries on the given server. reply will be populated by the
// receiver server.
func (r *Raft) appendEntries(server int, args appendEntriesArgs, reply *appendEntriesReply) {
	if ok := r.callAppendEntries(server, args, reply); !ok {
		glog.V(6).Infof("RPC to %d failed.", server)
		return
	}
}
