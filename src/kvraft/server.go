package kvraft

import (
	"bytes"
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
	committedRequest map[int]int
	lastReplyIndex   int
}

func (kv *KVServer) isRequestCommitted(clientId int, requestId int) bool {
	lastResponse, existClient := kv.committedRequest[clientId]
	if existClient && lastResponse >= requestId {
		return true
	}
	return false
}
func (kv *KVServer) newCommited(clientId int, requestId int) {
	kv.committedRequest[clientId] = requestId
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
			if apply.CommandValid {
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
						if _, exist := kv.keyValueMap[opCommand.Key]; !exist {
							kv.keyValueMap[opCommand.Key] = ""
						}
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
						go func(ch chan Respond, res Respond) {
							ch <- res
						}(ch, res)
					}
				}
				if kv.maxraftstate > 0 && kv.rf.GetPersistSize() > kv.maxraftstate {
					kv.takeSnapshot(apply.CommandIndex)
				}
				kv.mu.Unlock()

			} else if apply.SnapshotValid {
				kv.mu.Lock()
				if apply.SnapshotIndex <= kv.lastReplyIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastReplyIndex = apply.SnapshotIndex
				kv.restoreSnapshot(apply.Snapshot)
				kv.mu.Unlock()
			}
		} else {
			panic("channel close error")
		}
		//time.Sleep(1 * time.Millisecond)
	}
}

func (kv *KVServer) takeSnapshot(index int) {
	KVSnapshot := KVSnapshot{KeyValueMap: kv.keyValueMap, CommittedRequest: kv.committedRequest}
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(KVSnapshot)
	kv.rf.Snapshot(index, w.Bytes())
}
func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	var b bytes.Buffer
	dec := labgob.NewDecoder(&b)
	b.Write(snapshot)
	var data KVSnapshot
	err := dec.Decode(&data)
	if err != nil {
		panic("Decode error\n")
	}
	kv.keyValueMap = data.KeyValueMap
	kv.committedRequest = data.CommittedRequest
}

func (kv *KVServer) show() {
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
	DPrintf("Server%2d Start\n", me)
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
	kv.committedRequest = make(map[int]int)
	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.commitProcess()
	//go kv.garbageCleanProcess()

	return kv
}
