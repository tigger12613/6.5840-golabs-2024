package shardkv

import (
	"fmt"
	"time"

	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrStaleConfig = "ErrStaleConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CliendId  int
	RequestId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CliendId  int
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	PutCommand    = "Put"
	AppendCommand = "Append"
	GetCommand    = "Get"
	NewConfig     = "NewConfig"
	NewShard      = "NewShard"
	GiveShard     = "GiveShard"
)

type OpCommand string

type Respond struct {
	Err   Err
	Value string
}

type PushShardArgs struct {
	Config           shardctrler.Config
	Shards           map[int]*Shard
	CommittedRequest map[int]int
}

type PushShardReply struct {
	Err Err
}
type KVSnapshot struct {
	Shards           map[int]*Shard
	CommittedRequest map[int]int
	LastConfig       shardctrler.Config
	CurrentConfig    shardctrler.Config
}
type Shard struct {
	Num          int
	KV           map[string]string
	ReadyToServe bool
}

func (shard *Shard) deepCopy() *Shard {
	newShard := Shard{
		KV:           make(map[string]string),
		ReadyToServe: false,
	}
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	newShard.Num = shard.Num
	newShard.ReadyToServe = shard.ReadyToServe
	return &newShard
}
func (shard *Shard) String() string {
	return fmt.Sprintf("{Num:%d, KV:%+v, ReadyToServe:%t}", shard.Num, shard.KV, shard.ReadyToServe)
}
func shardsToString(shards map[int]*Shard) string {
	var s string
	for k, v := range shards {
		s += fmt.Sprintf("{key:%v, shard:%s},", k, v.String())
	}
	return s
}
func deepCopy(oldMap map[int]*Shard) map[int]*Shard {
	newMap := make(map[int]*Shard)
	for k, v := range oldMap {
		newMap[k] = v.deepCopy()
	}
	return newMap
}

const TIMEOUT = 500 * time.Millisecond
