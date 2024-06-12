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
	applyCh   chan ApplyMsg
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Requested vote] %v, %+v\n", rf.me, args)
	rf.printCurrentStatus()
	if args.Term < rf.currentTerm {
		//DPrintf("[Late] %v, is late of term", args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		if args.Term > rf.currentTerm {
			DPrintf("[Late] %v, i am late of term\n", rf.me)
			rf.setCurrentTerm(args.Term)
			rf.changeState(Follower)
		}
		// vote to we does not voted, and the candidate has at least as up-to-date as receiver's log
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.lastLog().Term || (args.LastLogTerm == rf.lastLog().Term && args.LastLogIndex >= rf.lastLog().Index)) {
			DPrintf("[Vote] %v, vote to %v\n", rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.electionTimer = time.Now()
			return
		}
	}
	reply.Term = rf.currentTerm
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
	// means follower is stale, need to catch up
	if args.PrevLogIndex > rf.lastLog().Index {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	sIndex, log := rf.findLogByIndex(args.PrevLogIndex)
	if log.Term != args.PrevLogTerm {
		rf.logs = rf.logs[0:sIndex]
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// Heartbeat
	if args.Log == nil {
		// refresh commit index
		if args.LeaderCommit > rf.commitIndex {
			DPrintf("[Heartbeat] server %v, get append from %+v\n", rf.me, args)
			if args.LeaderCommit > rf.lastLog().Index {
				rf.commitIndex = rf.lastLog().Index
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("[Append] server %v, get append from %+v\n", rf.me, args)
	rf.logs = rf.logs[0 : sIndex+1]
	rf.logs = append(rf.logs, args.Log...)
	// refresh commit index
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.lastLog().Index {
			rf.commitIndex = rf.lastLog().Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.printCurrentStatus()
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applyEntryDaemon() {
	DPrintf("[Apply] %v start apply\n", rf.me)
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		_, applyLog := rf.findLogByIndex(rf.lastApplied + 1)
		rf.lastApplied += 1
		rf.mu.Unlock()
		newApply := ApplyMsg{
			CommandValid:  true,
			Command:       applyLog.Entry,
			CommandIndex:  applyLog.Index,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		DPrintf("[Apply] server %v apply %v\n", rf.me, rf.lastApplied)
		rf.applyCh <- newApply
		time.Sleep(20 * time.Millisecond)
	}
}

// use index to find slice index and log
func (rf *Raft) findLogByIndex(index int) (int, Log) {
	if index < 0 {
		DPrintf("index not found %v", index)
		rf.printCurrentStatus()
		panic(100)
	}
	first := rf.logs[0].Index
	return first + index, rf.logs[first+index]
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if rf.state != Leader {
		return index, term, isLeader
	}
	newLog := Log{
		Term:  rf.currentTerm,
		Index: rf.lastLog().Index + 1,
		Entry: command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.printCurrentStatus()
	index = newLog.Index
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
	DPrintf("[Kill] %v\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		duration := time.Since(rf.electionTimer)
		rf.mu.Unlock()
		if duration > time.Duration(600)*time.Millisecond {
			rf.startElection()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (r1.Int63() % 350)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setCurrentTerm(rf.currentTerm + 1)
	rf.votedMe[rf.me] = true
	rf.votedFor = rf.me
	rf.electionTimer = time.Now()
	rf.changeState(Candidate)
	DPrintf("[start election] %v, Term %v \n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, termWhenStartElection int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLog().Index,
				LastLogTerm:  rf.lastLog().Term,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, &args, &reply)
			//DPrintf("%v, get %v support\n", rf.me, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					rf.votedMe[i] = true
				}
				if reply.Term > rf.currentTerm {
					DPrintf("[late], %v late of term,%+v\n", rf.me, reply)
					rf.setCurrentTerm(reply.Term)
					rf.changeState(Follower)
					return
				}
				if rf.currentTerm == termWhenStartElection && rf.state == Candidate && (len(rf.votedMe) > rf.getN()/2) {
					rf.changeState(Leader)
				}
			} else {
				DPrintf("[Connect] vote fail to connect %v,%v\n", rf.me, i)
			}
		}(i, rf.currentTerm)
	}
}
func (rf *Raft) leaderProcess() {
	rf.mu.Lock()
	rf.electionTimer = time.Now()
	rf.nextIndex = make([]int, rf.getN())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLog().Index + 1
	}
	rf.matchIndex = make([]int, rf.getN())
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	valid := rf.state == Leader && rf.killed() == false
	rf.mu.Unlock()
	for valid {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			//wg.Add(1)

			rf.mu.Lock()
			if rf.lastLog().Index >= rf.nextIndex[i] {
				// append task
				sIndex, preLog := rf.findLogByIndex(rf.nextIndex[i] - 1)
				readyToGivenLogs := rf.logs[sIndex+1:]
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  preLog.Term,
					Log:          readyToGivenLogs,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				go func(i int, args AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						DPrintf("[Callback] leader %v get %+v\n", rf.me, reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.state != Leader {
							return
						}
						if reply.Success {
							rf.nextIndex[i] += len(readyToGivenLogs)
							rf.matchIndex[i] = rf.nextIndex[i] - 1
							// check is log send to most followers, we need to check all index that give to follower in this rpc
							for candidateIndexToCommit := readyToGivenLogs[0].Index; candidateIndexToCommit <= readyToGivenLogs[len(readyToGivenLogs)-1].Index; candidateIndexToCommit++ {
								_, candidatelog := rf.findLogByIndex(candidateIndexToCommit)
								if candidateIndexToCommit > rf.commitIndex && candidatelog.Term == rf.currentTerm {
									count := 0
									for _, mi := range rf.matchIndex {
										if mi >= candidateIndexToCommit {
											count++
										}
									}
									// +1 is for leader itseld
									if count+1 > rf.getN()/2 {
										DPrintf("[Commit] leader %v, commit index %v\n", rf.me, candidateIndexToCommit)
										rf.commitIndex = candidateIndexToCommit
									}
								}
							}
							rf.printCurrentStatus()
						} else {
							if reply.Term > rf.currentTerm {
								rf.setCurrentTerm(reply.Term)
								rf.changeState(Follower)
							} else {
								tmpSIndex := sIndex
								for ; rf.logs[tmpSIndex].Term == preLog.Term; tmpSIndex-- {

								}
								rf.nextIndex[i] = rf.logs[tmpSIndex].Index + 1
							}
						}

					} else {
						//DPrintf("[Connect] append fail to connect %v,%v", rf.me, i)
					}
				}(i, args)
			} else {
				// Heartbeat
				sIndex, preLog := rf.findLogByIndex(rf.nextIndex[i] - 1)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  preLog.Term,
					Log:          nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				go func(i int, args AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Leader {
						return
					}
					if ok {
						if reply.Success {

						} else {
							if reply.Term > rf.currentTerm {
								// my term is stale than other
								rf.setCurrentTerm(reply.Term)
								rf.changeState(Follower)
							} else {
								// the follower don't have preLog. We try jump to last term of preLog to quickly find the log follower have
								tmpSIndex := sIndex
								for ; rf.logs[tmpSIndex].Term == preLog.Term; tmpSIndex-- {

								}
								rf.nextIndex[i] = rf.logs[tmpSIndex].Index + 1
							}
						}
					} else {
						//DPrintf("[Connect] heartBeat fail to connect %v,%v", rf.me, i)
					}
				}(i, args)
			}
			//DPrintf("%v, heartbeat %v", rf.me, i)

		}
		//wg.Wait()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		valid = rf.state == Leader && rf.killed() == false
		rf.electionTimer = time.Now()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.changeState(Follower)
	DPrintf("%v, not leader anymore\n", rf.me)
	rf.mu.Unlock()
}
func (rf *Raft) changeState(nextState State) {
	switch nextState {
	case Leader:
		DPrintf("[Become leader] %v, term %v\n", rf.me, rf.currentTerm)
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
		DPrintf("%v, set term to %v\n", rf.me, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.votedMe = map[int]bool{}
	}
}
func (rf *Raft) getN() int {
	return len(rf.peers)
}

func (rf *Raft) lastLog() Log {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) printCurrentStatus() {
	DPrintf("[Status] me:%v currentTerm:%v votedFor:%v votedMe:%+v logs:%+v state:%+v commitIndex:%+v lastApplied:%+v nextIndex:%+v matchIndex%+v\n",
		rf.me, rf.currentTerm, rf.votedFor, rf.votedMe, rf.logs, rf.state, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}
func (rf *Raft) timelyLogRfStatus() {
	for rf.killed() == false {
		rf.printCurrentStatus()
		time.Sleep(10 * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("=======================Start=========================")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimer = time.Now()
	rf.votedFor = -1
	rf.votedMe = make(map[int]bool)
	rf.logs = []Log{Log{0, 0, nil}}
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.changeState(Follower)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntryDaemon()

	return rf
}
