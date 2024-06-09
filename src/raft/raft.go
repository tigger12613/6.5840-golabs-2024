package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Log struct {
	Term  int
	Index int
	Entry interface{}
}
type State string

const (
	Leader    State = "leader"
	Follower  State = "follower"
	Candidate State = "candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	// when currentTerm increase votedFor should set to -1
	votedFor      int
	votedMe       map[int]bool
	logs          []Log
	state         State
	electionTimer time.Time
	// volatile
	commitIndex int
	lastApplied int
	// for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	DPrintf("[To vote] %v", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//DPrintf("%v, is late of term", args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		if args.Term > rf.currentTerm {
			DPrintf("%v, i am late of term", rf.me)
			rf.setCurrentTerm(args.Term)
		}
		// vote to we does not voted, and the candidate has at least as up-to-date as receiver's log
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			DPrintf("[Vote] %v, vote to %v", rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.electionTimer = time.Now()
			return
		}
		// if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1)) {
		// 	DPrintf("[Vote] %v, vote to %v", rf.me, args.CandidateId)
		// 	rf.votedFor = args.CandidateId
		// 	reply.VoteGranted = true
		// 	reply.Term = rf.currentTerm
		// 	rf.electionTimer = time.Now()
		// 	return
		// }
	}
	reply.VoteGranted = false
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []Log
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Append] server %v, get append from %v\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimer = time.Now()
	rf.changeState(Follower)
	if rf.currentTerm < args.Term {
		rf.setCurrentTerm(args.Term)
	}
	// if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.logs) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logs = rf.logs[0:args.PrevLogIndex]
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.logs = rf.logs[0 : args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Log...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[Kill] %v", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		duration := time.Since(rf.electionTimer)
		if duration > time.Duration(300)*time.Millisecond {
			rf.startElection()
		}
		switch rf.state {
		case Leader:
		case Candidate:
			if len(rf.votedMe) > rf.getN()/2 {
				rf.changeState(Leader)
			}
		case Follower:
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setCurrentTerm(rf.currentTerm + 1)
	rf.votedMe[rf.me] = true
	rf.electionTimer = time.Now()
	rf.changeState(Candidate)
	DPrintf("[start election] %v, Term %v \n", rf.me, rf.currentTerm)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.logs[len(rf.logs)-1].Index,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			//DPrintf("%v, get %v support\n", rf.me, reply)
			if ok {
				if reply.VoteGranted {
					rf.votedMe[i] = true
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					return
				}
			}
		}(i)
	}
	rf.electionTimer = time.Now()
	//DPrintf("%v, get %v support\n", rf.me, rf.votedMe)
	if len(rf.votedMe) > len(rf.peers)/2 {
		rf.changeState(Leader)
	}
}
func (rf *Raft) leaderProcess() {
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for rf.state == Leader && rf.killed() == false {
		rf.electionTimer = time.Now()
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Log:          rf.logs[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			//DPrintf("%v, heartbeat %v", rf.me, i)
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok {

			}
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.changeState(Follower)
	DPrintf("%v, not leader anymore\n", rf.me)
	rf.mu.Unlock()
}
func (rf *Raft) changeState(nextState State) {
	switch nextState {
	case Leader:
		DPrintf("[Become leader] %v, term %v", rf.me, rf.currentTerm)
		rf.state = Leader
		go rf.leaderProcess()
	case Candidate:
		rf.state = Candidate
	case Follower:
		rf.state = Follower
	}
}
func (rf *Raft) setCurrentTerm(term int) {
	if rf.currentTerm < term {
		DPrintf("%v, set term to %v", rf.me, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.votedMe = map[int]bool{}
	}
}
func (rf *Raft) getN() int {
	return len(rf.peers)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimer = time.Now()
	rf.votedFor = -1
	rf.votedMe = make(map[int]bool)
	rf.logs = []Log{Log{0, 0, nil}}
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.changeState(Follower)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
