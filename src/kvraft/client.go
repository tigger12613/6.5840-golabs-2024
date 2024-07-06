package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId      int
	clientId      int
	nextRequestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.nextRequestId = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		CliendId:  ck.clientId,
		RequestId: ck.nextRequestId,
	}
	ck.nextRequestId++
	DPrintf("Client: new get %+v\n", args)
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				DPrintf("Client: get ok %+v\n", args)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("Client: get no key %+v\n", args)
				return ""
			} else if reply.Err == ErrWrongLeader {
				DPrintf("Client: get wrong leader %+v\n", args)
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(1 * time.Millisecond)
	}
	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		CliendId:  ck.clientId,
		RequestId: ck.nextRequestId,
	}
	ck.nextRequestId++
	if op == "Put" {
		DPrintf("Client: new put %+v\n", args)
	} else {
		DPrintf("Client: new append %+v\n", args)
	}
	for {
		reply := PutAppendReply{}
		if op == "Put" {
			ok := ck.servers[ck.leaderId].Call("KVServer.Put", &args, &reply)
			if ok {
				if reply.Err == OK {
					DPrintf("Client: put ok %+v\n", args)
					return
				} else if reply.Err == ErrNoKey {
					panic("put append no key")
				} else if reply.Err == ErrWrongLeader {
					DPrintf("Client: put wrong leader %+v\n", args)
				}
			}
		} else if op == "Append" {
			reply := PutAppendReply{}
			ok := ck.servers[ck.leaderId].Call("KVServer.Append", &args, &reply)
			if ok {
				if reply.Err == OK {
					DPrintf("Client: append ok %+v\n", args)
					return
				} else if reply.Err == ErrNoKey {
					panic("put append no key")
				} else if reply.Err == ErrWrongLeader {
					DPrintf("Client: append wrong leader %+v\n", args)
				}
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
