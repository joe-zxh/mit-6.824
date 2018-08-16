package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64
	reqid int
	mu sync.Mutex
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
	// You'll have to add code here.
	ck.id = nrand()
	ck.reqid = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{Key:key, Id:ck.id}
	ck.mu.Lock()
	args.ReqID = ck.reqid
	ck.reqid++
	ck.mu.Unlock()

	for {
		for _, v:=range ck.servers {
			reply := GetReply{}
			ok := v.Call("RaftKV.Get", &args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Value //一直循环到成功PutAppend为止。
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// 对所有server都call一次，如果不是leader，PutAppend的时候会自动忽略请求。 leader会使用那个Start(command)函数，然后让所有的节点都执行这条指令。
	args := PutAppendArgs{Key:key, Value:value, Op:op, Id:ck.id}

	ck.mu.Lock()
	args.ReqID = ck.reqid
	ck.reqid++
	ck.mu.Unlock()

	for {
		for _, v:=range ck.servers {
			reply := PutAppendReply{}
			ok := v.Call("RaftKV.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false {
				return //一直循环到成功PutAppend为止。
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
