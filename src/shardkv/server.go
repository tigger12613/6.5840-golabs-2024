package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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
	Command          OpCommand
	Key              string
	Value            string
	ClientId         int
	RequestId        int
	NewConfig        shardctrler.Config
	NewShards        map[int]*Shard
	CommittedRequest map[int]int
	GiveShards       int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	sc            *shardctrler.Clerk

	// Your definitions here.
	dead   int32 // set by Kill()
	shards map[int]*Shard
	rpcs   map[int]chan Respond
	// key: client id, value: last commited operation of the client
	committedRequest map[int]int
	lastReplyIndex   int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if kv.currentConfig.Shards[key2shard(args.Key)] != kv.gid || !kv.shards[key2shard(args.Key)].ReadyToServe {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

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
	//DPrintf("Gid: %2d, Server:%2d, Get %+v", kv.gid, kv.me, args)
	myChan := kv.getNewRpcCh(index)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.currentConfig.Shards[key2shard(args.Key)] != kv.gid || !kv.shards[key2shard(args.Key)].ReadyToServe {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	var command OpCommand
	if args.Op == "Put" {
		command = PutCommand
	} else if args.Op == "Append" {
		command = AppendCommand
	} else {
		panic("PutAppend: wrong operation")
	}
	op := Op{
		Command:   command,
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
	//DPrintf("Gid: %2d, Server:%2d, Put %+v", kv.gid, kv.me, args)
	myChan := kv.getNewRpcCh(index)
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
func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	// if args.Config.Num < kv.currentConfig.Num {
	// 	reply.Err = ErrStaleConfig
	// 	kv.mu.Unlock()
	// 	return
	// }
	// if args.Config.Num > kv.currentConfig.Num {
	// 	ok := kv.newConfig(args.Config)
	// 	if !ok {
	// 		reply.Err = ErrWrongLeader
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	op := Op{
		Command:          NewShard,
		NewShards:        deepCopy(args.Shards),
		CommittedRequest: args.CommittedRequest,
	}
	index, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//DPrintf("Gid: %2d, Server:%2d, PushShard: %+v", kv.gid, kv.me, args)
	myChan := kv.getNewRpcCh(index)
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

func (kv *ShardKV) newConfig(newConfig shardctrler.Config) bool {
	op := Op{
		Command:   NewConfig,
		NewConfig: newConfig,
	}
	_, _, ok := kv.rf.Start(op)
	return ok
}

func (kv *ShardKV) getNewRpcCh(index int) chan Respond {
	kv.rpcs[index] = make(chan Respond)
	return kv.rpcs[index]
}

func (kv *ShardKV) isRequestCommitted(clientId int, requestId int) bool {
	lastResponse, existClient := kv.committedRequest[clientId]
	if existClient && lastResponse >= requestId {
		return true
	}
	return false
}
func (kv *ShardKV) newCommited(clientId int, requestId int) {
	kv.committedRequest[clientId] = requestId
}

func (kv *ShardKV) commitProcess() {
	for !kv.killed() {
		if apply, ok := <-kv.applyCh; ok {
			if apply.CommandValid {

				kv.mu.Lock()
				opCommand := apply.Command.(Op)
				if apply.CommandIndex <= kv.lastReplyIndex {
					kv.mu.Unlock()
					continue
				}
				_, isLeader := kv.rf.GetState()
				kv.lastReplyIndex = apply.CommandIndex
				if opCommand.Command == PutCommand || opCommand.Command == AppendCommand {
					if !kv.isRequestCommitted(opCommand.ClientId, opCommand.RequestId) {
						belongShard := key2shard(opCommand.Key)
						if kv.currentConfig.Shards[belongShard] == kv.gid && kv.shards[belongShard].ReadyToServe {
							switch opCommand.Command {
							case PutCommand:
								kv.shards[belongShard].KV[opCommand.Key] = opCommand.Value
							case AppendCommand:
								if _, exist := kv.shards[belongShard].KV[opCommand.Key]; !exist {
									kv.shards[belongShard].KV[opCommand.Key] = ""
								}
								kv.shards[belongShard].KV[opCommand.Key] += opCommand.Value
							}
							kv.newCommited(opCommand.ClientId, opCommand.RequestId)
						}
					}
				} else {
					switch opCommand.Command {
					case NewConfig:
						if opCommand.NewConfig.Num < kv.currentConfig.Num {
							break
						}
						kv.lastConfig = kv.currentConfig
						kv.currentConfig = opCommand.NewConfig
						if kv.currentConfig.Num == 1 {
							for i := 0; i < shardctrler.NShards; i++ {
								if kv.currentConfig.Shards[i] == kv.gid {
									kv.shards[i] = &Shard{KV: map[string]string{}, ReadyToServe: true}
								}
							}
						}
						if isLeader {
							DPrintf("Gid: %2d, Server:%2d, Configchange:%+v", kv.gid, kv.me, kv.currentConfig)
							DPrintf("Gid: %2d, Server:%2d, shards:%+v", kv.gid, kv.me, kv.shards)
							//go kv.giveShards()
						}
					case NewShard:
						if isLeader {
							DPrintf("Gid: %2d, Server:%2d, GetShard:%+v", kv.gid, kv.me, opCommand.NewShards)
						}
						for k, v := range opCommand.NewShards {
							if kv.shards[k].Num < v.Num {
								kv.shards[k] = v.deepCopy()
								kv.shards[k].ReadyToServe = true
							}
						}
						for clientId, id := range opCommand.CommittedRequest {
							if kv.committedRequest[clientId] < id {
								kv.committedRequest[clientId] = id
							}
						}
					case GiveShard:
						if isLeader {
							DPrintf("Gid: %2d, Server:%2d, GiveShard:%+v", kv.gid, kv.me, opCommand.GiveShards)
						}
						kv.shards[opCommand.GiveShards].ReadyToServe = false
					}
				}
				ch, exist := kv.rpcs[apply.CommandIndex]

				res := Respond{Value: "", Err: ErrWrongLeader}
				if isLeader {
					if opCommand.Command == PutCommand || opCommand.Command == AppendCommand || opCommand.Command == GetCommand {
						belongShard := key2shard(opCommand.Key)
						if kv.currentConfig.Shards[belongShard] == kv.gid && kv.shards[belongShard].ReadyToServe {
							switch opCommand.Command {
							case GetCommand:
								val, exist := kv.shards[belongShard].KV[opCommand.Key]
								if exist {
									res.Value, res.Err = val, OK
								} else {
									res.Value, res.Err = "", ErrNoKey
								}
								//DPrintf("Gid: %2d, Server:%2d, Key:%s, Get:%s\n", kv.gid, kv.me, opCommand.Key, kv.shards[belongShard].KV[opCommand.Key])
								DPrintf("Gid: %2d, Server:%2d, Key:%s, Get:%s\n", kv.gid, kv.me, opCommand.Key, shardsToString(kv.shards))

							case PutCommand:
								DPrintf("Gid: %2d, Server:%2d, Key:%s, Put:%s\n", kv.gid, kv.me, opCommand.Key, kv.shards[belongShard].KV[opCommand.Key])

								res.Err = OK
							case AppendCommand:
								DPrintf("Gid: %2d, Server:%2d, Key:%s, Append:%s\n", kv.gid, kv.me, opCommand.Key, kv.shards[belongShard].KV[opCommand.Key])

								res.Err = OK
							}
						} else {
							res.Err = ErrWrongGroup
						}
					} else {
						switch opCommand.Command {
						case NewShard:
							res.Err = OK
						default:
							res.Err = OK
						}
					}
				}
				if exist {
					go func(ch chan Respond, res Respond) {
						ch <- res
					}(ch, res)
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
func (kv *ShardKV) checkConfigChangeProcess() {
	for !kv.killed() {
		kv.mu.Lock()
		tmp := kv.sc.Query(kv.currentConfig.Num + 1)
		if tmp.Num > kv.currentConfig.Num {
			//DPrintf("Gid: %2d, Server: %2d, NewConfig: %+v", kv.gid, kv.me, tmp)
			kv.newConfig(tmp)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) giveShardsProcess() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if isLeader {
			go kv.giveShards(kv.currentConfig.Num)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) giveShards(configNum int) {
	kv.mu.Lock()
	for i, shard := range kv.shards {
		targetGid := kv.currentConfig.Shards[i]
		if kv.currentConfig.Num != configNum {
			break
		}
		if targetGid != kv.gid && shard.ReadyToServe {
			shard.Num = kv.currentConfig.Num
			tmp := make(map[int]*Shard)
			tmp[i] = shard.deepCopy()
			for _, targetName := range kv.currentConfig.Groups[targetGid] {
				args := PushShardArgs{Config: kv.currentConfig, Shards: tmp, CommittedRequest: kv.committedRequest}
				reply := PushShardReply{}
				kv.mu.Unlock()
				ok := kv.make_end(targetName).Call("ShardKV.PushShard", &args, &reply)
				kv.mu.Lock()
				if ok && reply.Err == OK {
					op := Op{
						Command:    GiveShard,
						GiveShards: i,
					}
					kv.rf.Start(op)
					break
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

			}
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) takeSnapshot(index int) {
	KVSnapshot := KVSnapshot{Shards: kv.shards, CommittedRequest: kv.committedRequest, CurrentConfig: kv.currentConfig, LastConfig: kv.lastConfig}
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(KVSnapshot)
	kv.rf.Snapshot(index, w.Bytes())
}
func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
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
	kv.shards = data.Shards
	kv.committedRequest = data.CommittedRequest
	kv.currentConfig = data.CurrentConfig
	kv.lastConfig = data.LastConfig
}

func (kv *ShardKV) show() {
	DPrintf("Server%2d,keyValueMap:%+v,rpcs:%+v, lastReplyIndex:%v", kv.me, kv.shards, kv.rpcs, kv.lastReplyIndex)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("Gid:%d,Server:%2d, Killed\n", kv.gid, kv.me)
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.sc = shardctrler.MakeClerk(ctrlers)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastConfig = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: make(map[int][]string)}
	kv.currentConfig = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: make(map[int][]string)}
	kv.shards = make(map[int]*Shard)
	kv.rpcs = make(map[int]chan Respond)
	kv.committedRequest = make(map[int]int)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = &Shard{KV: map[string]string{}, ReadyToServe: false}
	}

	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.commitProcess()
	go kv.checkConfigChangeProcess()
	go kv.giveShardsProcess()

	return kv
}
