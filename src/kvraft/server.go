package kvraft

import (
	"log"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   OpCommand
	Key       string
	Value     string
	ClientId  int
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValueMap      map[string]string
	rpcs             map[int]chan Respond
	committedRequest map[int]map[int]bool
	lastReplyIndex   int
}

func (kv *KVServer) isRequestCommitted(clientId int, requestId int) bool {
	_, existClient := kv.committedRequest[clientId]
	if existClient {
		_, existRequest := kv.committedRequest[clientId][requestId]
		return existRequest
	}
	return false
}
func (kv *KVServer) newCommited(clientId int, requestId int) {
	_, existClient := kv.committedRequest[clientId]
	if !existClient {
		kv.committedRequest[clientId] = make(map[int]bool)
	}
	kv.committedRequest[clientId][requestId] = true
}
func (kv *KVServer) getRpcCh(index int) chan Respond {
	if _, ok := kv.rpcs[index]; !ok {
		kv.rpcs[index] = make(chan Respond)
	}
	return kv.rpcs[index]
}
func (kv *KVServer) getNewRpcChAndRemoveOld(index int) chan Respond {
	if _, ok := kv.rpcs[index]; ok {
		kv.rpcs[index] <- Respond{Value: "", Err: ErrWrongLeader}
	}
	kv.rpcs[index] = make(chan Respond)
	return kv.rpcs[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	op := Op{
		Command:   GetCommand,
		Key:       args.Key,
		ClientId:  args.CliendId,
		RequestId: args.RequestId,
	}
	index, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server:%2d, Get %+v", kv.me, args)
	myChan := kv.getNewRpcChAndRemoveOld(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.rpcs, index)
		kv.mu.Unlock()
	}()

	select {
	case respond := <-myChan:
		reply.Value, reply.Err = respond.Value, respond.Err
		return
	case <-time.After(TIMEOUT):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		Command:   PutCommand,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.CliendId,
		RequestId: args.RequestId,
	}
	index, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server:%2d, Put %+v", kv.me, args)
	myChan := kv.getNewRpcChAndRemoveOld(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.rpcs, index)
		kv.mu.Unlock()
	}()
	select {
	case respond := <-myChan:
		reply.Err = respond.Err
		return
	case <-time.After(TIMEOUT):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	op := Op{
		Command:   AppendCommand,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.CliendId,
		RequestId: args.RequestId,
	}
	index, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("Server:%2d, Append %+v", kv.me, args)
	myChan := kv.getNewRpcChAndRemoveOld(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.rpcs, index)
		kv.mu.Unlock()
	}()

	select {
	case respond := <-myChan:
		reply.Err = respond.Err
		return
	case <-time.After(TIMEOUT):
		reply.Err = ErrWrongLeader
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("Server:%2d, Killed\n", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) commitProcess() {
	for !kv.killed() {
		if apply, ok := <-kv.applyCh; ok {
			DPrintf("Server:%2d, Commit %+v", kv.me, apply)
			kv.mu.Lock()
			opCommand := apply.Command.(Op)
			if apply.CommandIndex <= kv.lastReplyIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastReplyIndex = apply.CommandIndex
			if !kv.isRequestCommitted(opCommand.ClientId, opCommand.RequestId) {
				switch opCommand.Command {
				case PutCommand:
					kv.keyValueMap[opCommand.Key] = opCommand.Value
				case AppendCommand:
					kv.keyValueMap[opCommand.Key] += opCommand.Value
				}
				kv.newCommited(opCommand.ClientId, opCommand.RequestId)
			}

			ch, exist := kv.rpcs[apply.CommandIndex]
			_, isLeader := kv.rf.GetState()
			res := Respond{Value: "", Err: ErrWrongLeader}
			if isLeader {
				switch opCommand.Command {
				case GetCommand:
					val, exist := kv.keyValueMap[opCommand.Key]
					if exist {
						res.Value, res.Err = val, OK
					} else {
						res.Value, res.Err = "", ErrNoKey
					}
				case PutCommand:
					res.Err = OK
				case AppendCommand:
					res.Err = OK
				}
				if exist {
					ch <- res
				}
			}
			kv.mu.Unlock()
		} else {
			break
		}
		//time.Sleep(1 * time.Millisecond)
	}
}

func (kv *KVServer) cleanup() {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		for _, v := range kv.rpcs {
			v <- Respond{Value: "", Err: ErrWrongLeader}
		}
	}
	kv.rpcs = make(map[int]chan Respond)
}

// func (kv *KVServer) garbageCleanProcess() {
// 	for !kv.killed() {
// 		kv.mu.Lock()
// 		_, isFollower := kv.rf.GetState()
// 		for k := range kv.rpcs {
// 			if k < kv.lastId || isFollower {
// 				close(kv.rpcs[k].channel)
// 				delete(kv.rpcs, k)
// 			}
// 		}
// 		//kv.snap()
// 		kv.mu.Unlock()
// 		time.Sleep(300 * time.Millisecond)
// 	}
// }

func (kv *KVServer) snap() {
	DPrintf("Server%2d,keyValueMap:%+v,rpcs:%+v, lastReplyIndex:%v", kv.me, kv.keyValueMap, kv.rpcs, kv.lastReplyIndex)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.keyValueMap = make(map[string]string)
	kv.rpcs = make(map[int]chan Respond)
	kv.committedRequest = make(map[int]map[int]bool)

	go kv.commitProcess()
	//go kv.garbageCleanProcess()

	return kv
}
