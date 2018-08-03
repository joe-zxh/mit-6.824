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
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

const HEARTBEAT_TIME int = 50 // leader50ms发送一次心跳

const PRINTNUM int = 10 // 打印最后10条日志项

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
type Raft struct { // 要保存的量 的首字母要 变成大写
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 论文里面的量
	CurrentTerm int
	VotedFor int
	Logs []Entry //key是index  value是操作，这里用一个整数来表示的。

	CommitIndex int
	CommitTerm int //对应commitIndex的term，防止重复添加的 (TestRejoin)
	LastApplied int //应该在ApplyMsg里面要用到

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
	return rf.CurrentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//fmt.Printf("server[%d]:Saving Persist...\n", rf.me)

	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.CommitTerm)
	e.Encode(rf.LastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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
	fmt.Printf("server[%d]:Reading Persist...\n", rf.me)

	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.CommitTerm)
	d.Decode(&rf.LastApplied)

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


	lastLog := rf.Logs[len(rf.Logs)-1]

	if (args.Term>rf.CurrentTerm){ //收到更新的term之后，降级为follower
		rf.CurrentTerm = args.Term
		rf.status = FOLLOWER
		rf.VotedFor=-1
		rf.persist()
	}

	if (rf.VotedFor==-1 && args.Term>=rf.CurrentTerm && (args.LastLogTerm>lastLog.Term || (args.LastLogTerm==lastLog.Term && args.LastLogIndex>=lastLog.Index))){
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	} else{
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm

	//fmt.Printf("节点[%d]收到[%d]的RequestVote, 投票:%t 投票给了:%d 当前term: %d\n", rf.me, args.CandidateId, reply.VoteGranted, rf.VotedFor, rf.CurrentTerm)

	//fmt.Printf("节点[%d]收到[%d]的RequestVote, 节点的term:%d, 节点最新log的term:%d, index:%d, args.term:%d, LastLogTerm:%d, LastLogIndex:%d 投票:%t 投票给了:%d\n",
	//	rf.me, args.CandidateId, rf.CurrentTerm, lastLog.Term, lastLog.Index, args.Term, args.LastLogTerm, args.LastLogIndex, reply.VoteGranted, rf.VotedFor)
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

	if (reply.Term>rf.CurrentTerm) {
		rf.status = FOLLOWER
		rf.VotedFor = -1
		rf.CurrentTerm = reply.Term
		rf.persist()
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
	appendEntry:=Entry{rf.CurrentTerm, len(rf.Logs), command }

	rf.Logs = append(rf.Logs, appendEntry)
	rf.logAppendNum[len(rf.Logs)-1] = 1 //初始化为1

	rf.persist()
	printLogEnd(rf, PRINTNUM)

	return len(rf.Logs)-1, rf.CurrentTerm, isLeader
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

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = make([]Entry,1) // 为了从1开始索引，在index=0的位置加了一个空的日志项
	rf.logAppendNum = make(map[int]int)

	rf.CommitIndex = 0 //根据论文里面的图，都是从1开始计数的
	rf.CommitTerm = 0
	rf.LastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.status = FOLLOWER //初始化为follower
	rf.beLeader = make(chan bool, 1)
	rf.getHeartBeat = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	printLogEnd(rf, PRINTNUM)

	go func(rf *Raft){ //Make() must return quickly, so it should start goroutines for any long-running work.
		for{
			switch rf.status{
			case FOLLOWER:
				select {
				case <-time.After(getRandomExpireTime()): // 接收Leader的心跳超时，变成candidate，准备选举
					rf.mu.Lock() // 这个lock是因为不止一个go routine在使用当前的节点，例如在 labrpc.go的MakeNetwork()那里的ProcessReq可能会用到当前的节点
					rf.VotedFor = -1
					rf.persist()
					rf.status = CANDIDATE
					fmt.Printf("节点[%d]接收leader心跳超时, 变成candidate, 时间: %v\n", rf.me, time.Now().UnixNano()/1000000) //毫秒级别
					//rf.CurrentTerm = rf.CurrentTerm + 1 //选举之前，先自增一下任期, 这个自增的位置，和师兄的代码的位置不同!!!
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
					fmt.Printf("节点[%d]成为leader, 时间: %v, 当前任期: %d\n", rf.me, time.Now().UnixNano()/1000000, rf.CurrentTerm) //毫秒级别

					for i:=0;i<len(rf.peers); i++{ //初始化一波nextIndex
						rf.nextIndex[i] = len(rf.Logs)
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

	if (args.Term<rf.CurrentTerm) {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.status = CANDIDATE
		rf.voteCount = 0
		fmt.Printf("AppendEntries - Server [%d] becomes CANDIDATE.\n", rf.me)
		return
	}

	if (args.Term>rf.CurrentTerm) {
		rf.status = FOLLOWER
		fmt.Printf("AppendEntries - Server [%d] becomes FOLLOWER, Current Time: %v\n", rf.me, time.Now().UnixNano()/1000000)
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		reply.Term = args.Term
		rf.persist()
	}

	reply.Term = rf.CurrentTerm
	rf.getHeartBeat <- true

	//更新一下commitIndex 以及commitTerm
	if (args.LeaderCommitIndex>rf.CommitIndex) {

		oldCommitIndex:=rf.CommitIndex//这个是为了通过测试用的, 测试要求：commit的顺序要逐个逐个递增(但其实只需要递增即可，不需要逐个逐个来)

		newCommitIndex:=-1

		//if (args.LeaderCommitIndex > len(rf.Logs)-1) {
		//	newCommitIndex = len(rf.Logs)-1
		//} else
		// 这边的话，leader发来的index值一定要<=当前节点的index才行，不然无法保证当前节点是否和leader拥有在commitIndex处拥有相同的日志项
		if (args.LeaderCommitIndex <= len(rf.Logs)-1&&args.LeaderCommitTerm==rf.Logs[args.LeaderCommitIndex].Term){ //要检查一下leader已经commit的日志项是否 和 当前节点拥有的对应index的日志项是同一个项
			newCommitIndex = args.LeaderCommitIndex
		}

		if (newCommitIndex!=-1){
			rf.CommitIndex = newCommitIndex
			for i:=oldCommitIndex+1;i<=rf.CommitIndex;i++{
				sendApplyMsg:=ApplyMsg{i, rf.Logs[i].Command, false, []byte{}}
				rf.applyCh <- sendApplyMsg
			}

			rf.persist()
			fmt.Printf("from follower [%d] : CommitIndex: %d\n", rf.me, rf.CommitIndex)
		}
	}

	if (args.Entries.Index==-1){ //心跳
		return
	}

	if (args.PrevLogIndex > len(rf.Logs)-1) { //index不存在
		reply.Success = false
		return
	} else if(rf.Logs[args.PrevLogIndex].Term!=args.PrevLogTerm){ //index存在 但term不相等
		reply.Success = false
		return
	} else { //match
		reply.Success = true

		if (len(rf.Logs)>args.Entries.Index) { //判断 是覆盖，还是添加
			if(rf.Logs[args.Entries.Index].Term==args.Entries.Term&&rf.Logs[args.Entries.Index].Index==args.Entries.Index) { //避免重复添加
				return
			}
			////!!!!!!!!!!
			rf.Logs[args.Entries.Index] = args.Entries //覆盖
		} else {
			rf.Logs = append(rf.Logs, args.Entries) //添加
		}
		rf.persist()
		printLogEnd(rf, PRINTNUM)
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (!ok || rf.status!=LEADER) { // 如果 RPC 请求失败 或者 当前节点已经不是leader，直接返回。
		return false
	}

	if (reply.Term>rf.CurrentTerm) {
		rf.CurrentTerm = reply.Term
		rf.status = FOLLOWER
		rf.VotedFor = -1
		rf.persist()
		return false
	}

	if (reply.Success) {
		if (args.Entries.Index!=-1) { //不是心跳
			rf.matchIndex[server] = args.Entries.Index
			rf.nextIndex[server] = rf.matchIndex[server]+1

			ind:=args.Entries.Index
			rf.logAppendNum[ind]++


			if (rf.logAppendNum[ind]>len(rf.peers)/2  && ind > rf.CommitIndex && rf.Logs[ind].Term==rf.CurrentTerm) { // 那么把 ind之前的项都commit掉

				for i:=rf.CommitIndex+1;i<=ind;i++ {
					rf.CommitIndex = i
					rf.CommitTerm = rf.Logs[i].Term
					fmt.Printf("from leader [%d] : CommitIndex: %d  CommitTerm: %d\n", rf.me, rf.CommitIndex, rf.CommitTerm)

					rf.persist()
					sendApplyMsg:=ApplyMsg{rf.CommitIndex, rf.Logs[rf.CommitIndex].Command, false, []byte{}}
					rf.applyCh <- sendApplyMsg
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
			prevLog:=rf.Logs[rf.nextIndex[i]-1]
			lastLog:=rf.Logs[len(rf.Logs)-1]

			entrySend:=Entry{}

			if (lastLog.Index>=prevLog.Index+1 && rf.matchIndex[i]!=lastLog.Index) { //rf.matchIndex[i]!=lastLog.Index是用来判断，follower的日志是和leader的完全一样 还是 只是index一样
				entrySend=rf.Logs[rf.nextIndex[i]]
			} else { //follower节点的日志长度和leader的一样的时候, 发送一条空的entry表示heartbeat
				entrySend.Index=-1 //index=-1表示是心跳
			}

			args:=AppendEntriesArgs{rf.CurrentTerm, rf.me, prevLog.Index, prevLog.Term, entrySend, rf.CommitIndex, rf.CommitTerm}
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
	LeaderCommitTerm int
}

type AppendEntriesReply struct{
	Term int
	Success bool
}

// candidate的leader选举
func election(rf *Raft) {
	rf.mu.Lock()
	rf.CurrentTerm++ //选举之前，先自增一下任期, 这个自增的位置，不能在外面，不然过不了test2!!!
	rf.VotedFor = rf.me
	rf.voteCount = 1
	rf.persist()
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
				lastLog:=rf.Logs[len(rf.Logs)-1]
				args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLog.Index, lastLog.Term}
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
	for _, entry:=range rf.Logs {
		fmt.Printf("i:%d t:%d c:%v -> ", entry.Index, entry.Term, entry.Command)
	}
	fmt.Println()
}

// 只打印最后n条日志项
func printLogEnd(rf *Raft, n int) {
	fmt.Printf("server[%d]: ", rf.me)

	var i int
	if (len(rf.Logs)>=(n+1)) {
		i = len(rf.Logs)-(n+1)
	} else {
		i = 0
	}

	for ;i<len(rf.Logs);i++ {
		fmt.Printf("i:%d t:%d c:%v -> ", rf.Logs[i].Index, rf.Logs[i].Term, rf.Logs[i].Command)
	}

	fmt.Println()
}