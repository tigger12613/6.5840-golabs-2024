package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const TIMEOUT = 500 * time.Millisecond

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
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
)

type OpCommand string

type IndexTerm struct {
	Index int
	Term  int
}

type RpcHandle struct {
	Id      int
	channel chan Respond
}
type Respond struct {
	Err   Err
	Value string
}
