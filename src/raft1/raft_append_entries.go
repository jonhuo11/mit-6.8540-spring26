package raft

type AppendEntriesArgs struct {
	Term              uint
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       uint
	Entries           []logEntry
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
		return
	}

	r.gotHeartbeat = true

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.raftRole = raftRoleFollower
	}

	// TODO: log related stuff

	reply.Success = true
}

// no lock, concurrent safe
// TODO: might need to build a retry mechanism, somewhere else?
// leader retries until entries are replicated on >half the servers
func (r *Raft) sendAppendEntries(to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[to].Call("Raft.AppendEntries", args, reply)
	return ok
}
