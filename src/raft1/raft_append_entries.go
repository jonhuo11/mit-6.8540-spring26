package raft

type AppendEntriesArgs struct {
	term              uint
	leaderId          int
	prevLogIndex      int
	prevLogTerm       uint
	entries           []string
	leaderCommitIndex uint
}

type AppendEntriesReply struct {
	term    uint // helps the calling leader resolve split brain (if the calling leader finds out its behind, it reverts to follower)
	success bool
}

// AppendEntries RPC

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.term < r.currentTerm {
		reply.success = false
		return
	}

	// TODO: log related stuff

	reply.success = true
}
