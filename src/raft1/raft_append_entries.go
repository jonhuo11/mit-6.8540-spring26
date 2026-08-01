package raft

import "6.5840/raftapi"

type AppendEntriesArgs struct {
	Term              uint
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       uint
	Entries           []LogEntry
	LeaderCommitIndex uint
}

type AppendEntriesReply struct {
	Term    uint // helps the calling leader resolve split brain (if the calling leader finds out its behind, it reverts to follower)
	Success bool
}

// AppendEntries RPC
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return // don't listen to a non-leader
	}

	r.suppressElection = true

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.raftRole = raftRoleFollower
		r.votedFor = uncastVote
	}

	// ===== log related stuff =====
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(r.log) {
		reply.Success = false
		return
	}
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// from the paper: leader will repeatedly decrement the args.prevLogIndex until this does match,
		// then the next step is to overwrite all logs on the follower from there on with the leader's
		return
	}
	// overwrite the next N
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(r.log) && r.log[idx].Term == e.Term {
			continue
		}
		r.log = append(r.log[:idx], args.Entries[i:]...)
		break
	}
	if args.LeaderCommitIndex > r.commitIndex {
		r.commitIndex = min(args.LeaderCommitIndex, uint(args.PrevLogIndex+len(args.Entries)))
	}

	// actually apply to state machine
	for r.commitIndex > r.lastApplied { // leader applying to self
		r.lastApplied++
		r.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      r.log[r.lastApplied].Value,
			CommandIndex: int(r.lastApplied),
		}
	}

	reply.Success = true
}

// no lock, concurrent safe
// TODO: might need to build a retry mechanism, somewhere else?
// leader retries until entries are replicated on >half the servers
func (r *Raft) sendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[to].Call("Raft.AppendEntries", args, reply)
	return ok
}
