package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint
	CandidateId  int
	LastLogIndex int
	LastLogTerm  uint
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint
	VoteGranted bool // true => they granted us a vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor != uncastVote {
		// TODO: once logs are added, the candidate log must be as up to date as the receiver
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}
