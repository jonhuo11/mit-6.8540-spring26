package raft

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
	i, j := 0, 0
	for i = args.PrevLogIndex + 1; i < len(r.log); i++ {
		j = i - (args.PrevLogIndex + 1) // parallel log index in the entries array
		if r.log[i].Term == args.Entries[j].Term {
			continue
		}
		// this is the divergence point, we overwrite from here with the leader's logs args.Entries[k]
		break
	}
	r.log = append(r.log[:i], args.Entries[j:]...)
	if args.LeaderCommitIndex > r.commitIndex {
		r.commitIndex = min(args.LeaderCommitIndex, uint(len(r.log)-1))
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
