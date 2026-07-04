package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type raftRole = uint8

const (
	raftRoleLeader raftRole = iota
	raftRoleFollower
	raftRoleCandidate
)

const (
	// lab limits leader heartbeats to no more than 10 per second
	minHeartbeatsPerSec float32 = 7
	maxHeartbeatsPerSec float32 = 9

	// use one that is not >5 seconds or else you will fail to elect a leader
	minElectionTimeoutPerSec float32 = 0.9
	maxElectionTimeoutPerSec float32 = 1.1
)

const uncastVote int = -1

type logEntry struct {
	value string
	term  uint
}

// A Go object implementing a single Raft peer.
type Raft struct {
	/*
		Jonathan's notes:
		Can the mutex be unfair, and in a pathological case block the heartbeat/election events?

		sync.Mutex has had a starvation mode since Go 1.9:
			If a waiter has been blocked for more than 1ms,
			the mutex switches to direct handoff and waiters are served FIFO.
			So a goroutine can't be starved indefinitely by barging.
			Worst case it eats some milliseconds of extra latency under heavy contention

		So no, the lock acquisition is "fair" and the time won't mess things up.

		Raft is designed to be tolerant of timing issues, only requirement is:
			broadcastTime ≪ electionTimeout ≪ MTBF
	*/
	mu sync.Mutex // Lock to protect shared access to this peer's state

	// concurrent safe
	peers []*labrpc.ClientEnd // RPC end points of all peers, never modified so concurrent safe

	// concurrent safe
	me  int  // this peer's index into peers[]
	n   uint // n total servers
	maj uint // N/2 + 1 if even, otherwise (N+1)/2

	persister *tester.Persister // Object to hold this peer's persisted state

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm uint
	votedFor    int // the candidate ID that received a vote on the current term. -1 if none
	log         []logEntry

	// volatile on followers
	commitIndex uint // highest known commit index
	lastApplied uint // actually applied to state machine

	// volatile on leaders (reinitialized after election)
	nextIndexForFollowers  []uint // index of the next log entry to send to that server (initially leader last log index + 1)
	matchIndexForFollowers []uint // index of highest log entry we know is replicated on each follower

	// custom state
	raftRole     raftRole
	gotHeartbeat bool // since the current election timeout started ticking, did we get a heartbeat?
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.raftRole == raftRoleLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// This ONLY RUNS ON LEADERs
func (r *Raft) onHeartbeatTicker() {
	r.mu.Lock()
	if r.raftRole != raftRoleLeader { // only leaders send heartbeats
		r.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:     r.currentTerm,
		LeaderId: r.me,

		// TODO: other fields
	}

	r.mu.Unlock()
	r.sendHeartbeatToAllPeers(&args)
}

// only the leader may call this function
func (r *Raft) sendHeartbeatToAllPeers(args *AppendEntriesArgs) {
	for peerId := range r.peers {
		reply := AppendEntriesReply{}
		if ok := r.sendAppendEntries(peerId, args, &reply); !ok {
			fmt.Printf("leader %v failed to send heartbeat RPC to follower %v\n", r.me, peerId)
			continue
		}
		// are we out of term? => drop to follower, stop
		r.mu.Lock() // wakeup, check validity of leadership
		if reply.Term > r.currentTerm {
			r.raftRole = raftRoleFollower
			r.gotHeartbeat = true
			r.mu.Unlock()
			return
		}
		if r.raftRole != raftRoleLeader {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		// keep sending heartbeats
	}
}

/*
What is an election? As a candidate, you
1) Increment own term
2) Vote for self
3) Reset election timer
4) Send RequestVote RPCs to all other servers
5) If votes recv'd from majority of servers, become leader
*/
func (r *Raft) onElectionTimeout() {
	r.mu.Lock()

	if r.raftRole == raftRoleLeader {
		r.mu.Unlock()
		return
	}

	if r.raftRole == raftRoleFollower && (r.gotHeartbeat || r.votedFor != uncastVote) {
		// follower got heartbeat, keep following
		r.gotHeartbeat = false
		r.votedFor = uncastVote
		r.mu.Unlock()
		return
	}
	r.gotHeartbeat = false // reset for next cycle

	// If election timeout elapses without either:
	// - receiving AppendEntries RPC from current leader (heartbeat) OR
	// - granting a vote to a candidate
	// => Convert to candidate
	// This code also works for existing candidates (starts a new election if the current one timed out)
	r.raftRole = raftRoleCandidate

	r.currentTerm += 1
	r.votedFor = r.me
	var votesRecvdThisTerm uint = 1

	r.mu.Unlock()

	args := RequestVoteArgs{}
	for peerId := range r.peers {
		reply := RequestVoteReply{}
		if ok := r.sendRequestVote(peerId, &args, &reply); !ok {
			fmt.Printf("candidate %v failed to call RequestVote RPC on peer %v\n", r.me, peerId)
			continue
		}

		r.mu.Lock()
		if reply.Term > r.currentTerm {
			// drop to follower
			r.currentTerm = reply.Term
			r.raftRole = raftRoleFollower
			r.mu.Unlock()
			return
		}
		if r.raftRole != raftRoleCandidate { // still a candidate? didn't get dropped to follower by some other RPC?
			r.mu.Unlock()
			return
		}
		if !reply.VoteGranted {
			r.mu.Unlock()
			return
		}
		votesRecvdThisTerm += 1
		if votesRecvdThisTerm >= r.maj {
			// become leader
			r.raftRole = raftRoleLeader
			r.votedFor = uncastVote
			args := AppendEntriesArgs{ // initial empty heartbeat to assert leadership
				Term:     r.currentTerm,
				LeaderId: r.me,
			}
			r.mu.Unlock()
			r.sendHeartbeatToAllPeers(&args)
			return
		}
		r.mu.Unlock()
	}

	// could not gather enough votes, a new election will start next timeout cycle
}

func calcTpsDelayMs(tps float32) float32 {
	out := 1000.0 / tps
	return out
}

// The tester requires that the leader send heartbeats no more than 10/min
func ticker(onTicker func(), minTicksPerSecond, maxTicksPerSecond float32) { // election timeout ticker
	if minTicksPerSecond > maxTicksPerSecond {
		panic("minTicksPerSecond > maxTicksPerSecond")
	}
	maxTpsDelayMs := calcTpsDelayMs(minTicksPerSecond)
	minTpsDelayMs := calcTpsDelayMs(maxTicksPerSecond)
	delta := maxTpsDelayMs - minTpsDelayMs
	for {
		// Your code here (3A)
		// Check if a leader election should be started.
		onTicker()

		// pause for a random amount of time between
		ms := rand.Int63n(int64(delta)) + int64(minTpsDelayMs)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.n = uint(len(peers))
	if rf.n%2 == 0 {
		rf.maj = (rf.n / 2) + 1
	} else {
		rf.maj = (rf.n + 1) / 2
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// the leader may only send up to 10 heartbeats per sec
	// you must elect a leader within 5 seconds of past leader failing
	go ticker(rf.onElectionTimeout, minElectionTimeoutPerSec, maxElectionTimeoutPerSec)

	// start leader heartbeat ticker
	go ticker(rf.onHeartbeatTicker, minHeartbeatsPerSec, maxHeartbeatsPerSec)

	return rf
}
