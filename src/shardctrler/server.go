package shardctrler

import (
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	committedRequest map[int]int
	rpcs             map[int]chan Respond
	lastReplyIndex   int
	configs          []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Command   OpCommand
	Servers   map[int][]string // Join
	GIDs      []int            // Leave
	Shard     int              // Move
	GID       int              // Move
	Num       int              //Query
	ClientId  int
	RequestId int
}

func (sc *ShardCtrler) isRequestCommitted(clientId int, requestId int) bool {
	lastResponse, existClient := sc.committedRequest[clientId]
	if existClient && lastResponse >= requestId {
		return true
	}
	return false
}
func (sc *ShardCtrler) newCommited(clientId int, requestId int) {
	sc.committedRequest[clientId] = requestId
}
func (sc *ShardCtrler) getRpcCh(index int) chan Respond {
	if _, ok := sc.rpcs[index]; !ok {
		sc.rpcs[index] = make(chan Respond)
	}
	return sc.rpcs[index]
}
func (sc *ShardCtrler) getNewRpcCh(index int) chan Respond {
	sc.rpcs[index] = make(chan Respond)
	return sc.rpcs[index]
}
func (sc *ShardCtrler) lastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	op := Op{
		Command:   OpJoin,
		Servers:   copyGroups(args.Servers),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("Me:%2d, Join %+v\n", sc.me, args)
	myChan := sc.getNewRpcCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.rpcs, index)
		sc.mu.Unlock()
	}()

	select {
	case respond := <-myChan:
		reply.Err = respond.Err
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
		return
	case <-time.After(TIMEOUT):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	op := Op{
		Command:   OpLeave,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("Me:%2d, Leave %+v\n", sc.me, args)
	myChan := sc.getNewRpcCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.rpcs, index)
		sc.mu.Unlock()
	}()

	select {
	case respond := <-myChan:
		reply.Err = respond.Err
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
		return
	case <-time.After(TIMEOUT):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	op := Op{
		Command:   OpMove,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("Me:%2d, Move %+v\n", sc.me, args)
	myChan := sc.getNewRpcCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.rpcs, index)
		sc.mu.Unlock()
	}()

	select {
	case respond := <-myChan:
		reply.Err = respond.Err
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
		return
	case <-time.After(TIMEOUT):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	op := Op{
		Command:   OpQuery,
		Num:       args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("Me:%2d, Query %+v\n", sc.me, args)
	myChan := sc.getNewRpcCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.rpcs, index)
		sc.mu.Unlock()
	}()
	select {
	case respond := <-myChan:
		reply.Config, reply.Err = respond.Config, respond.Err
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
		return
	case <-time.After(TIMEOUT):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	DPrintf("Server:%2d, Killed\n", sc.me)
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}
func (sc *ShardCtrler) commitProcess() {
	for !sc.killed() {
		if apply, ok := <-sc.applyCh; ok {
			if apply.CommandValid {

				sc.mu.Lock()
				operation := apply.Command.(Op)
				if apply.CommandIndex <= sc.lastReplyIndex {
					sc.mu.Unlock()
					continue
				}
				sc.lastReplyIndex = apply.CommandIndex
				if !sc.isRequestCommitted(operation.ClientId, operation.RequestId) {
					switch operation.Command {
					case OpJoin:
						lastConfig := sc.lastConfig()
						newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: copyGroups(lastConfig.Groups)}
						for gid, servers := range operation.Servers {
							if _, ok := newConfig.Groups[gid]; !ok {
								newServers := make([]string, len(servers))
								copy(newServers, servers)
								newConfig.Groups[gid] = newServers
							}
						}
						gidHaveShard := make(map[int][]int)
						for gid := range newConfig.Groups {
							gidHaveShard[gid] = make([]int, 0)
						}
						if len(lastConfig.Groups) == 0 {
							var keys []int
							for gid := range newConfig.Groups {
								keys = append(keys, gid)

							}
							sort.Ints(keys)
							for i := 0; i < NShards; i++ {
								gidHaveShard[keys[0]] = append(gidHaveShard[keys[0]], i)
							}
						} else {
							for i := 0; i < NShards; i++ {
								gidHaveShard[newConfig.Shards[i]] = append(gidHaveShard[newConfig.Shards[i]], i)
							}
						}
						//DPrintf("tmp %+v\n", gidHaveShard)

						for {
							if len(gidHaveShard) == 0 {
								break
							}
							maxGid, minGid := getMaxMinGid(gidHaveShard)
							if len(gidHaveShard[maxGid])-len(gidHaveShard[minGid]) <= 1 {
								break
							}
							sort.Ints(gidHaveShard[maxGid])
							gidHaveShard[minGid] = append(gidHaveShard[minGid], gidHaveShard[maxGid][0])
							gidHaveShard[maxGid] = gidHaveShard[maxGid][1:]
							if _, ok := sc.rf.GetState(); ok {
								DPrintf("gidHaveShard %+v\n", gidHaveShard)
							}
						}
						//DPrintf("gidHaveShard %+v\n", gidHaveShard)
						for gid, shards := range gidHaveShard {
							for _, val := range shards {
								newConfig.Shards[val] = gid
							}
						}
						sc.configs = append(sc.configs, newConfig)
					case OpLeave:
						lastConfig := sc.lastConfig()
						newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: copyGroups(lastConfig.Groups)}
						gidHaveShard := make(map[int][]int)
						for gid := range newConfig.Groups {
							gidHaveShard[gid] = make([]int, 0)
						}
						for i := 0; i < NShards; i++ {
							gidHaveShard[newConfig.Shards[i]] = append(gidHaveShard[newConfig.Shards[i]], i)
						}
						noManagerShard := make([]int, 0)
						for _, leaveGid := range operation.GIDs {
							delete(newConfig.Groups, leaveGid)
							noManagerShard = append(noManagerShard, gidHaveShard[leaveGid]...)
							delete(gidHaveShard, leaveGid)
						}
						//DPrintf("noManagerShard %+v\n", noManagerShard)
						sort.Ints(noManagerShard)
						for len(noManagerShard) > 0 {
							if len(gidHaveShard) == 0 {
								break
							}
							_, minGid := getMaxMinGid(gidHaveShard)
							gidHaveShard[minGid] = append(gidHaveShard[minGid], noManagerShard[0])
							noManagerShard = noManagerShard[1:]
						}

						//DPrintf("gidHaveShard %+v\n", gidHaveShard)
						for gid, shards := range gidHaveShard {
							for _, val := range shards {
								newConfig.Shards[val] = gid
							}
						}
						sc.configs = append(sc.configs, newConfig)
					case OpMove:
						lastConfig := sc.lastConfig()
						newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: copyGroups(lastConfig.Groups)}
						newConfig.Shards[operation.Shard] = operation.GID
						sc.configs = append(sc.configs, newConfig)
					case OpQuery:
					default:
					}
					sc.newCommited(operation.ClientId, operation.RequestId)
				}

				ch, exist := sc.rpcs[apply.CommandIndex]
				_, isLeader := sc.rf.GetState()
				res := Respond{Err: ErrWrongLeader}
				if isLeader {
					switch operation.Command {
					case OpJoin:
						res.Err = OK
					case OpLeave:
						res.Err = OK
					case OpMove:
						res.Err = OK
					case OpQuery:
						res.Err = OK
						if operation.Num == -1 {
							res.Config = sc.lastConfig()
						} else {
							res.Config = sc.configs[operation.Num]
						}
					default:
					}
					if exist {
						go func(ch chan Respond, res Respond) {
							ch <- res
						}(ch, res)
					}
				}
				DPrintf("Me:%2d,Comand: %+v,Config: %+v\n", sc.me, operation, sc.lastConfig())
				sc.mu.Unlock()
			}
		} else {
			panic("channel close error")
		}
		//time.Sleep(1 * time.Millisecond)
	}
}
func getMaxMinGid(gidHaveShard map[int][]int) (int, int) {
	maxGid := 0
	maxCount := 0
	minGid := math.MaxInt32
	minCount := math.MaxInt32
	for gid, shards := range gidHaveShard {
		if len(shards) > maxCount {
			maxCount = len(shards)
			maxGid = gid
		} else if len(shards) == maxCount {
			if gid < maxGid {
				maxCount = len(shards)
				maxGid = gid
			}
		}
		if len(shards) < minCount {
			minCount = len(shards)
			minGid = gid
		} else if len(shards) == minCount {
			if gid < minGid {
				minCount = len(shards)
				minGid = gid
			}
		}
	}
	return maxGid, minGid
}
func copyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for key, value := range groups {
		tmp := make([]string, len(value))
		copy(tmp, value)
		newGroups[key] = tmp
	}
	return newGroups
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.rpcs = make(map[int]chan Respond)
	sc.committedRequest = make(map[int]int)
	go sc.commitProcess()

	return sc
}
