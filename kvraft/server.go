package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters, otherwise RPC will break.

	Key   string
	Value string
	Kind    string //put或者append

	Id int64 // 发出请求的client的Id
	ReqId int // Id为client的RequestId的值
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // 这个channel和raft服务器的是 同一个，所以raft apply的时候，这个也会收到。

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string] string
	ack map[int64]int // key是client的Id  value是client的requestId
	result map[int]chan Op // 用来接收
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {

	index, _, isLeader := kv.rf.Start(entry)

	if !isLeader{
		return false
	}

	kv.mu.Lock()
	ch, ok:= kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op:=<-ch: // 接收到entry
		return op==entry //检查一下apply的是否和发送的请求里面的entry相同
	case <-time.After(1000*time.Millisecond): //超时了
		return false
	}
}

func (kv *RaftKV) CheckDup(id int64, reqid int) bool {
	v, ok := kv.ack[id]

	if ok {
		return v >= reqid
	}
	return false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	entry := Op{Key:args.Key, Kind:"Get", Id: args.Id, ReqId:args.ReqID}

	ok := kv.AppendEntryToLog(entry)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK

		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.ReqID

		kv.mu.Unlock()
	}

}

func (kv *RaftKV) Apply(args Op) {
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.ReqId // 放在外面，是因为还有get的情况需要考虑。
}


func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	entry := Op{args.Key, args.Value, args.Op, args.Id, args.ReqID}

	ok := kv.AppendEntryToLog(entry)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	go func(){

		for{
			msg := <-kv.applyCh


			op:=msg.Command.(Op)

			kv.mu.Lock()

			ch, ok := kv.result[msg.Index]

			if (!kv.CheckDup(op.Id, op.ReqId)) { // 如果不是duplicated的，才apply它
				kv.Apply(op)
			}

			if ok { // 这个是leader，且发出了那个index的command才会ok
				ch <-op
			}

			kv.mu.Unlock()

		}


	}()

	return kv
}
