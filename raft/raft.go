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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"

const HEARTBEAT_TIME int = 50 // leader50ms发送一次心跳


// 这个是用来测试用的 在config.go里面的start1()里面有个go func()检查
// as(尽管) each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
// 某个节点执行(apply)了一条已经commit的指令
// 尽管它知道后面还有已经commit但还没执行的指令，但它还需要发送一条AppllyMsg给自己?
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Status int

const( //模拟一个枚举类型，表示节点状态
	FOLLOWER Status = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 论文里面的量
	currentTerm int
	votedFor int
	logs []Entry //key是index  value是操作，这里用一个整数来表示的。

	commitIndex int
	lastApplied int //应该在ApplyMsg里面要用到

	//leader要用到的量，用来处理日志不一致的
	nextIndex []int
	matchIndex []int

	// 别的一些量
	status Status // 记录自身是leader、candidate、follower中哪个状态

	beLeader chan bool // requestVote之后，接收到超过半数节点的投票之后，往这个channel里面 放点东西
	getHeartBeat chan bool // 接收到一个appendEntry的请求，就相当于 接收到一个leader的 心跳。这个量是为了当节点是follower或candidate时，不跳进 Make()里面 go func()里面 timeout的那个case里面的
	voteCount int // 得到的票数
	voteReplyOkCount int // 收到有效回复的总数
	voteReplyCount int // 收到回复的总数

	logAppendNum map[int]int // 已经append了这条索引为index的日志项的节点的个数

	applyCh chan ApplyMsg
}

type Entry struct { //因为要进行RPC通讯，所以要大写
	Term int
	Index int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here.

	if (rf.status==LEADER) {
		isleader = true
	}
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()


	lastLog := rf.logs[len(rf.logs)-1]

	if (args.Term>rf.currentTerm){ //收到更新的term之后，降级为follower
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor=-1
	}

	if (rf.votedFor==-1 && args.Term>=rf.currentTerm && (args.LastLogTerm>lastLog.Term || (args.LastLogTerm==lastLog.Term && args.LastLogIndex>=lastLog.Index))){
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else{
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm

	fmt.Printf("节点[%d]收到[%d]的RequestVote, 投票:%t 投票给了:%d 当前term: %d\n", rf.me, args.CandidateId, reply.VoteGranted, rf.votedFor, rf.currentTerm)

	//fmt.Printf("节点[%d]收到[%d]的RequestVote, 节点的term:%d, 节点最新log的term:%d, index:%d, args.term:%d, LastLogTerm:%d, LastLogIndex:%d 投票:%t 投票给了:%d\n",
	//	rf.me, args.CandidateId, rf.currentTerm, lastLog.Term, lastLog.Index, args.Term, args.LastLogTerm, args.LastLogIndex, reply.VoteGranted, rf.votedFor)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock() //这个锁要放在Call的后面，不然会死锁
	defer rf.mu.Unlock()

	if (!ok) { // 如果请求发送失败，直接返回。
		rf.voteReplyCount++
		return ok
	} else {
		rf.voteReplyCount++
		rf.voteReplyOkCount++
	}

	if (reply.Term>rf.currentTerm) {
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		return ok
	}

	if (reply.VoteGranted && rf.status == CANDIDATE) {
		rf.voteCount++

		//fmt.Printf("节点[%d]收到的投票数: %d  回复总数: %d  有效回复总数: %d   %v  %v\n", rf.me, rf.voteCount, rf.voteReplyCount, rf.voteReplyOkCount, rf.voteReplyCount==len(rf.peers), rf.voteCount> rf.voteReplyOkCount/2)
		// 因为test2里面用了长延时，所以这样还是拿不到 回复的总数。不过可以尝试在外面一层 用超时来计算。但这样有点复杂

		if(rf.voteCount> len(rf.peers)/2){ //大于一半的有效的节点投票了
			rf.beLeader <- true
		}
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever(可能?) committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader:=true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.status!=LEADER){
		isLeader = false
		return index, term, isLeader
	}
	appendEntry:=Entry{rf.currentTerm, len(rf.logs), command }

	rf.logs = append(rf.logs, appendEntry)
	rf.logAppendNum[len(rf.logs)-1] = 1 //初始化为1

	printLog(rf)

	for i:=range(rf.peers) {
		if (i!=rf.me && rf.status==LEADER) {
			prevLog:=rf.logs[len(rf.logs)-2]

			args:=AppendEntriesArgs{rf.currentTerm, rf.me, prevLog.Index, prevLog.Term, appendEntry, rf.commitIndex}
			reply:=AppendEntriesReply{}

			go func(server int) {
				rf.sendAppendEntries(server, args, &reply)
			}(i)
		}
	}
	return len(rf.logs)-1, rf.currentTerm, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry,1) // 为了从1开始索引，在index=0的位置加了一个空的日志项
	rf.logAppendNum = make(map[int]int)

	rf.commitIndex = 0 //根据论文里面的图，都是从1开始计数的
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.status = FOLLOWER //初始化为follower
	rf.beLeader = make(chan bool, 1)
	rf.getHeartBeat = make(chan bool, 1)

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	printLog(rf)

	go func(rf *Raft){ //Make() must return quickly, so it should start goroutines for any long-running work.
		for{
			switch rf.status{
			case FOLLOWER:
				select {
				case <-time.After(getRandomExpireTime()): // 接收Leader的心跳超时，变成candidate，准备选举
					rf.mu.Lock() // 这个lock是因为不止一个go routine在使用当前的节点，例如在 labrpc.go的MakeNetwork()那里的ProcessReq可能会用到当前的节点
					rf.votedFor = -1
					rf.status = CANDIDATE
					fmt.Printf("节点[%d]接收leader心跳超时, 变成candidate, 时间: %v\n", rf.me, time.Now().UnixNano()/1000000) //毫秒级别
					//rf.currentTerm = rf.currentTerm + 1 //选举之前，先自增一下任期, 这个自增的位置，和师兄的代码的位置不同!!!
					rf.mu.Unlock()

				case <-rf.getHeartBeat: //这是为了不进入那个上面超时的case
				}

			case LEADER:
				broadcastAppendEntries(rf) //复制leader的日志项给 followers
				time.Sleep(time.Duration(HEARTBEAT_TIME)*time.Millisecond)

			case CANDIDATE:
				election(rf)

				select {
				case <-rf.beLeader:
					rf.mu.Lock()
					rf.status = LEADER
					fmt.Printf("节点[%d]成为leader, 时间: %v\n", rf.me, time.Now().UnixNano()/1000000) //毫秒级别

					for i:=0;i<len(rf.peers); i++{ //初始化一波nextIndex
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0 //根据论文，index应该是从1开始计数的
					}
					rf.mu.Unlock()

				case <-rf.getHeartBeat: //接收到别的leader的心跳，表示选举成功了，下面要降级为follower
					//其实 可以在AppendEntries的时候 已经判断了一下term
					rf.mu.Lock()
					rf.status = FOLLOWER
					rf.mu.Unlock()

					//case <-time.After(500*time.Millisecond)://超时再没有接收到 有效的消息，那么 重新选举
				case <-time.After(getRandomExpireTime())://超时再没有接收到 有效的消息，那么 重新选举

				}
			}
		}
	}(rf)

	return rf
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	if (args.Term<rf.currentTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.status = CANDIDATE
		rf.voteCount = 0
		fmt.Printf("AppendEntries - Server [%d] becomes CANDIDATE.\n", rf.me)
		return
	}

	if (args.Term>rf.currentTerm) {
		rf.status = FOLLOWER
		fmt.Printf("AppendEntries - Server [%d] becomes FOLLOWER, Current Time: %v\n", rf.me, time.Now().UnixNano()/1000000)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}

	rf.getHeartBeat <- true

	if (args.LeaderCommitIndex>rf.commitIndex) {

		if (args.LeaderCommitIndex< len(rf.logs)-1) {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = len(rf.logs)-1
		}

		sendApplyMsg:=ApplyMsg{rf.commitIndex, rf.logs[rf.commitIndex].Command, false, []byte{}}

		rf.applyCh <- sendApplyMsg

		fmt.Printf("from follower [%d] : commitIndex: %d\n", rf.me, rf.commitIndex)
	}

	if (args.Entries.Index==-1){ //心跳
		return
	}

	if (args.PrevLogIndex > len(rf.logs)-1) { //index不存在
		reply.Success = false
		return
	} else if(rf.logs[args.PrevLogIndex].Term!=args.PrevLogTerm){ //index存在 但term不相等
		reply.Success = false
	} else { //match
		reply.Success = true

		if (len(rf.logs)>args.Entries.Index) { //判断 是覆盖，还是添加
			rf.logs[args.Entries.Index] = args.Entries
		} else {
			rf.logs = append(rf.logs, args.Entries)
		}

		printLog(rf)
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (!ok || rf.status!=LEADER) { // 如果 RPC 请求失败 或者 当前节点已经不是leader，直接返回。
		return false
	}

	if (reply.Term>rf.currentTerm) {
		rf.currentTerm = reply.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		return false
	}

	if (reply.Success) {
		if (args.Entries.Index!=-1) { //不是心跳
			rf.matchIndex[server] = args.Entries.Index
			rf.nextIndex[server] = rf.matchIndex[server]+1

			ind:=args.Entries.Index
			rf.logAppendNum[ind]++

			for i:=rf.commitIndex+1; i<len(rf.logs); i++ { //保证后面的项commit之前，前面的项都commit了
				if (rf.logAppendNum[i]>len(rf.peers)/2) {
					rf.commitIndex = i
					fmt.Printf("from leader [%d] : commitIndex: %d\n", rf.me, rf.commitIndex)

					sendApplyMsg:=ApplyMsg{rf.commitIndex, rf.logs[rf.commitIndex].Command, false, []byte{}}
					rf.applyCh <- sendApplyMsg
				} else {
					break
				}
			}
		}
	} else {
		if (rf.nextIndex[server]>1) {
			rf.nextIndex[server]--
		}
	}
	return true
}

func broadcastAppendEntries(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i:=range(rf.peers) {
		if (i!=rf.me && rf.status==LEADER) {
			prevLog:=rf.logs[rf.nextIndex[i]-1]
			lastLog:=rf.logs[len(rf.logs)-1]

			entrySend:=Entry{}

			if (lastLog.Index>=prevLog.Index+1 && rf.matchIndex[i]!=lastLog.Index) { //rf.matchIndex[i]!=lastLog.Index是用来判断，follower的日志是和leader的完全一样 还是 只是index一样
				entrySend=rf.logs[rf.nextIndex[i]]
			} else { //follower节点的日志长度和leader的一样的时候, 发送一条空的entry表示heartbeat
				entrySend.Index=-1 //index=-1表示是心跳
			}

			args:=AppendEntriesArgs{rf.currentTerm, rf.me, prevLog.Index, prevLog.Term, entrySend, rf.commitIndex}
			reply:=AppendEntriesReply{}

			go func(server int) {
				rf.sendAppendEntries(server, args, &reply)
			}(i)
		}
	}
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries Entry // index设为-1时 表示心跳
	LeaderCommitIndex int

}

type AppendEntriesReply struct{
	Term int
	Success bool
}

// candidate的leader选举
func election(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm++ //选举之前，先自增一下任期, 这个自增的位置，不能在外面，不然过不了test2!!!
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()

	go func(){
		broadcastRequestVote(rf)
	}()
}

func broadcastRequestVote(rf *Raft){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.voteReplyCount = 1 //投票给了自己
	rf.voteReplyOkCount = 1

	for i:=range rf.peers{
		if (i!=rf.me && rf.status==CANDIDATE){
			go func(server int){ //这个如果是 不带参数的函数，会有bug，我也不知道是为什么???
				lastLog:=rf.logs[len(rf.logs)-1]
				args := RequestVoteArgs{rf.currentTerm, rf.me, lastLog.Index, lastLog.Term}
				reply:=RequestVoteReply{}
				rf.sendRequestVote(server , args, &reply)
			}(i)
		}
	}
}

func getRandomExpireTime() time.Duration{ //150-300ms
	return time.Duration(rand.Int63n(300-150)+150)*time.Millisecond
}


func printLog(rf *Raft) {
	fmt.Printf("server[%d]: ", rf.me)
	for _, entry:=range rf.logs {
		fmt.Printf("i:%d t:%d c:%v -> ", entry.Index, entry.Term, entry.Command)
	}
	fmt.Println()
}