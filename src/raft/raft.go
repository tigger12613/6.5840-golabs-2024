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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	CurrentTerm int
	// when currentTerm increase VotedFor should set to -1
	VotedFor      int
	votedMe       map[int]bool
	Logs          []Log
	state         State
	electionTimer time.Time
	// volatile
	commitIndex int
	lastApplied int
	offset      int
	// for leader
	nextIndex  []int
	matchIndex []int

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
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
	// DPrintf("[Persist]")
	// rf.printCurrentStatus()
	//DPrintf("[Persist] me:%v, currrent term: %v, vote for: %v, logs: %+v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Read Persist] %v", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {

		panic(50)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
	}
	rf.printCurrentStatusStr("ReadPerisit")
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index == 0 {
		return
	}
	sIndex, log := rf.findLogByIndex(index)
	DPrintf("[Snapshot] %v snapshot %+v", rf.me, rf.Logs[0:sIndex+1])
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = log.Term
	first := rf.Logs[0]
	first.Index = index
	first.Term = log.Term
	rf.Logs = rf.Logs[sIndex+1:]
	rf.Logs = append([]Log{first}, rf.Logs...)
	DPrintf("[After Snapshot] %v remain %+v", rf.me, rf.Logs)
	rf.persist()
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
	rf.printCurrentStatusStr("Requested vote")
	if args.Term < rf.CurrentTerm {
		//DPrintf("[Late] %v, is late of term", args.CandidateId)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else {
		if args.Term > rf.CurrentTerm {
			DPrintf("[Late] %v, i am late of term\n", rf.me)
			rf.setCurrentTerm(args.Term)
			rf.changeState(Follower)
		}
		// vote to we does not voted, and the candidate has at least as up-to-date as receiver's log
		if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && (args.LastLogTerm > rf.lastLog().Term || (args.LastLogTerm == rf.lastLog().Term && args.LastLogIndex >= rf.lastLog().Index)) {
			DPrintf("[Vote] %v, vote to %v\n", rf.me, args.CandidateId)
			rf.persist()
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			rf.electionTimer = time.Now()
			return
		}
	}
	reply.Term = rf.CurrentTerm
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	DPrintf("[InstallSnapshot] %v %+v\n", rf.me, args)
	if args.Term > rf.CurrentTerm {
		rf.changeState(Follower)
		rf.setCurrentTerm(args.Term)
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		// rf.printCurrentStatusStr("CommitIndex")
		// panic(33)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	if args.LastIncludedIndex > rf.lastLog().Index {
		rf.Logs = make([]Log, 1)
	} else {
		rf.Logs = rf.Logs[args.LastIncludedIndex-rf.firstLog().Index:]
		rf.Logs[0].Entry = nil
	}

	rf.snapshot = args.Snapshot
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	newApply := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}
	rf.Logs[0].Index = rf.snapshotIndex
	rf.Logs[0].Term = rf.snapshotTerm
	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex
	rf.persist()
	rf.applyCh <- newApply
	reply.Term = rf.CurrentTerm
	reply.Success = true
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	XTerm   int
	XIntex  int
	//XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Append] server %v, get append from %+v\n", rf.me, args)
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	rf.resetElectionTimer()
	rf.changeState(Follower)
	if rf.CurrentTerm < args.Term {
		rf.setCurrentTerm(args.Term)
	}

	// if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// means follower is stale, need to catch up
	if args.PrevLogIndex > rf.lastLog().Index {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.XTerm = rf.lastLog().Term
		reply.XIntex = rf.lastLog().Index
		//reply.XLen = len(rf.Logs)
		return
	}
	sIndex, log := rf.findLogByIndex(args.PrevLogIndex)
	if log.Term != args.PrevLogTerm {
		rf.Logs = rf.Logs[0:sIndex]
		rf.persist()
		reply.Success = false
		reply.Term = rf.CurrentTerm
		i := sIndex - 1
		for ; i > 0 && rf.Logs[i].Term == rf.Logs[sIndex-1].Term; i-- {

		}
		reply.XTerm = rf.Logs[i].Term
		reply.XIntex = rf.Logs[i].Index
		//reply.XLen = len(rf.Logs)
		return
	}
	// Heartbeat
	if args.Log == nil {
		// refresh commit index
		if args.LeaderCommit > rf.commitIndex {
			//DPrintf("[Heartbeat] server %v, get append from %+v\n", rf.me, args)
			if args.LeaderCommit > rf.lastLog().Index {
				rf.commitIndex = rf.lastLog().Index
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
		reply.Success = true
		reply.Term = rf.CurrentTerm
		return
	}

	rf.Logs = rf.Logs[0 : sIndex+1]
	rf.Logs = append(rf.Logs, args.Log...)
	rf.persist()
	// refresh commit index
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.lastLog().Index {
			rf.commitIndex = rf.lastLog().Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.printCurrentStatusStr("AfterAppend")
	reply.Success = true
	reply.Term = rf.CurrentTerm
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
			time.Sleep(1 * time.Millisecond)
			continue
		}
		rf.printCurrentStatusStr("WhenApply")
		_, applyLog := rf.findLogByIndex(rf.lastApplied + 1)
		rf.lastApplied += 1
		newApply := ApplyMsg{
			CommandValid:  true,
			Command:       applyLog.Entry,
			CommandIndex:  applyLog.Index,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.mu.Unlock()
		DPrintf("[Apply] server %v apply %+v\n", rf.me, newApply)
		if newApply.Command != nil {
			rf.applyCh <- newApply
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// use index to find slice index and log
func (rf *Raft) findLogByIndex(index int) (int, Log) {
	if index < 0 {
		DPrintf("index not found %v", index)
		rf.printCurrentStatusStr("IndexNotFound")
		panic(100)
	}
	first := rf.Logs[0].Index
	return index - first, rf.Logs[index-first]
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
	term := rf.CurrentTerm
	isLeader := rf.state == Leader
	if rf.state != Leader {
		return index, term, isLeader
	}
	newLog := Log{
		Term:  rf.CurrentTerm,
		Index: rf.lastLog().Index + 1,
		Entry: command,
	}
	rf.Logs = append(rf.Logs, newLog)
	rf.persist()
	DPrintf("[New] server %v get new Log %+v \n", rf.me, newLog)
	//rf.printCurrentStatus()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[Kill] %v\n", rf.me)
	rf.printCurrentStatusStr("Kill")
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
	rf.setCurrentTerm(rf.CurrentTerm + 1)
	rf.votedMe[rf.me] = true
	rf.VotedFor = rf.me
	rf.electionTimer = time.Now()
	rf.changeState(Candidate)
	DPrintf("[start election] %v, Term %v \n", rf.me, rf.CurrentTerm)
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		t := rf.CurrentTerm
		rf.mu.Unlock()
		go func(i int, termWhenStartElection int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
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
				if reply.Term > rf.CurrentTerm {
					DPrintf("[late], %v late of term,%+v\n", rf.me, reply)
					rf.setCurrentTerm(reply.Term)
					rf.changeState(Follower)
					return
				}
				if rf.CurrentTerm == termWhenStartElection && rf.state == Candidate && (len(rf.votedMe) > rf.getN()/2) {
					rf.changeState(Leader)
				}
			} else {
				DPrintf("[Connect] vote fail to connect %v,%v\n", rf.me, i)
			}
		}(i, t)
	}
}
func fillIntSlice(slice []int, value int) {
	if slice == nil {
		return
	}
	for i := range slice {
		slice[i] = value
	}
}
func (rf *Raft) leaderProcess() {
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.nextIndex = make([]int, rf.getN())
	fillIntSlice(rf.nextIndex, rf.lastLog().Index+1)
	rf.matchIndex = make([]int, rf.getN())
	fillIntSlice(rf.matchIndex, 0)
	valid := rf.state == Leader && rf.killed() == false
	rf.mu.Unlock()
	for valid {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.nextIndex[i] <= rf.firstLog().Index {
				args := InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotIndex,
					LastIncludedTerm:  rf.snapshotTerm,
					Snapshot:          rf.snapshot,
				}
				rf.mu.Unlock()
				rf.leaderInstallSnapshot(i, args)
			} else if rf.lastLog().Index >= rf.nextIndex[i] {
				// append task
				sIndex, preLog := rf.findLogByIndex(rf.nextIndex[i] - 1)
				readyToGivenLogs := make([]Log, len(rf.Logs[sIndex+1:]))
				copy(readyToGivenLogs, rf.Logs[sIndex+1:])
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  preLog.Term,
					Log:          readyToGivenLogs,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				rf.leaderAppendEntry(i, args)
			} else {
				// Heartbeat
				preLog := rf.lastLog()
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: preLog.Index,
					PrevLogTerm:  preLog.Term,
					Log:          nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				rf.leaderHeartbeat(i, args)
			}
			//DPrintf("%v, heartbeat %v", rf.me, i)
		}
		//wg.Wait()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		valid = rf.state == Leader && rf.killed() == false
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.changeState(Follower)
	DPrintf("[Not Leader] %v, not leader anymore\n", rf.me)
	rf.mu.Unlock()
}
func (rf *Raft) leaderInstallSnapshot(peer int, args InstallSnapshotArgs) {

	go func(i int, args InstallSnapshotArgs) {
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(i, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				rf.nextIndex[i] = args.LastIncludedIndex + 1
			} else {
				if reply.Term > rf.CurrentTerm {
					rf.setCurrentTerm(reply.Term)
					rf.changeState(Follower)
				}
				rf.nextIndex[i] = args.LastIncludedIndex + 1
			}
		}
	}(peer, args)
}
func (rf *Raft) leaderAppendEntry(peer int, args AppendEntriesArgs) {
	go func(i int, args AppendEntriesArgs) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(i, &args, &reply)
		if ok {
			DPrintf("[Callback] leader %v get %+v\n", rf.me, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Leader || rf.CurrentTerm != args.Term {
				return
			}
			if reply.Success {
				rf.nextIndex[i] = args.Log[len(args.Log)-1].Index + 1
				rf.matchIndex[i] = args.Log[len(args.Log)-1].Index
				// check is log send to most followers, we need to check all index that give to follower in this rpc
				for candidateIndexToCommit := rf.commitIndex + 1; candidateIndexToCommit <= args.Log[len(args.Log)-1].Index; candidateIndexToCommit++ {
					_, candidatelog := rf.findLogByIndex(candidateIndexToCommit)
					if candidatelog.Term == rf.CurrentTerm {
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
				rf.printCurrentStatusStr("Commit")
			} else {
				if reply.Term > rf.CurrentTerm {
					rf.setCurrentTerm(reply.Term)
					rf.changeState(Follower)
				} else {
					rf.nextIndex[i] = reply.XIntex
				}
			}
		} else {
			//DPrintf("[Connect] append fail to connect %v,%v\n", rf.me, i)
		}
	}(peer, args)
}
func (rf *Raft) leaderHeartbeat(peer int, args AppendEntriesArgs) {
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
				if reply.Term > rf.CurrentTerm {
					// my term is stale than other
					rf.setCurrentTerm(reply.Term)
					rf.changeState(Follower)
				} else {
					// the follower don't have preLog. We try jump to last term of preLog to quickly find the log follower have
					rf.nextIndex[i] = reply.XIntex
				}
			}
		} else {
			//DPrintf("[Connect] heartBeat fail to connect %v,%v", rf.me, i)
		}
	}(peer, args)
}

func (rf *Raft) changeState(nextState State) {
	switch nextState {
	case Leader:
		DPrintf("[Become Leader] %v, term %v\n", rf.me, rf.CurrentTerm)
		rf.state = Leader
		go rf.leaderProcess()
	case Candidate:
		DPrintf("[Become Candidate] %v, term %v\n", rf.me, rf.CurrentTerm)
		rf.state = Candidate
	case Follower:
		DPrintf("[Become Follower] %v, term %v\n", rf.me, rf.CurrentTerm)
		rf.state = Follower
	}
}
func (rf *Raft) setCurrentTerm(term int) {
	if rf.CurrentTerm < term {
		DPrintf("[Change Term] %v, current term %v -> %v\n", rf.me, rf.CurrentTerm, term)
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.votedMe = map[int]bool{}
		rf.persist()
	}
}
func (rf *Raft) getN() int {
	return len(rf.peers)
}

func (rf *Raft) lastLog() Log {
	return rf.Logs[len(rf.Logs)-1]
}
func (rf *Raft) firstLog() Log {
	return rf.Logs[0]
}
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer = time.Now()
}

// func (rf *Raft) printCurrentStatus() {
// 	DPrintf("[Status] me:%v currentTerm:%v votedFor:%v votedMe:%+v state:%+v commitIndex:%+v lastApplied:%+v nextIndex:%+v matchIndex%+v logs:%+v\n",
// 		rf.me, rf.CurrentTerm, rf.VotedFor, rf.votedMe, rf.state, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.Logs)
// }
func (rf *Raft) printCurrentStatusStr(str string) {
	DPrintf("[%s][Status] me:%v currentTerm:%v votedFor:%v votedMe:%+v state:%+v commitIndex:%+v lastApplied:%+v nextIndex:%+v matchIndex%+v logs:%+v\n",
		str, rf.me, rf.CurrentTerm, rf.VotedFor, rf.votedMe, rf.state, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.Logs)
}

// func (rf *Raft) timelyLogRfStatus() {
// 	for rf.killed() == false {
// 		rf.printCurrentStatus()
// 		time.Sleep(10 * time.Millisecond)
// 	}
// }

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
	DPrintf("=======================Start=========================\n")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (3A, 3B, 3C).
	rf.electionTimer = time.Now()
	rf.VotedFor = -1
	rf.votedMe = make(map[int]bool)
	rf.Logs = []Log{Log{0, 0, nil}}
	rf.CurrentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.changeState(Follower)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.commitIndex = rf.Logs[0].Index
	rf.lastApplied = rf.Logs[0].Index
	rf.printCurrentStatusStr("Make")
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntryDaemon()

	return rf
}
