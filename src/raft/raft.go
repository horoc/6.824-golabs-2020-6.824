package raft

import "sync"
import "sync/atomic"
import "github.com/horoc/6.824-golabs-2020-6.824/src/labrpc"

//https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md 具体实现可以参考论文

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

//ApplyMsg实际上就是发送到状态机的信息，每个被commit的日志最后都要放到状态机里执行，状态机比如一个kv数据库
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//日志存储格式
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Data     interface{}
}

type PeerState int32

const (
	LEADER    PeerState = 0
	FOLLOWER  PeerState = 1
	CANDIDATE PeerState = 2
)

//Raft实例数据结构
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state PeerState //节点状态

	//所有Server所要存储的信息
	//1. 持久化的数据
	currentTerm int //当前任期号
	voteFor     int //当前获得选票的候选人id
	log         []LogEntry
	//2.存在内存中的数据
	commitIndex int //最大已经被提交的日志索引号。与[]logEntry中的最大索引号不同，log数组中的元素不一定是commit的，是否commit是由leader控制的，leader会根据matchIndex数组中节点的信息来确认commitIndex值
	//比如，各个节点的log数组的最大索引分别是（10,11,10,13,10), 那么leader就会将commitIndex拉到10，随后广播各个节点，各个节点也将commitIndex更新到10
	lastApplied int //应用到状态机的最大索引号

	//当Server成为leader后所要维护的信息（全部在内存中）
	nextIndex  []int //对每个Server, Leader要发送给他的下一个日志索引号
	matchIndex []int //对每个Server, 已经复制的最大日志索引号

}

//Rpc数据结构
//1. 请求投票的RPC request 和 response
type RequestVoteArgs struct {
	Term         int //currentTerm，会在成为候选节点时++
	CandidateId  int //候选人的id
	LastLogIndex int //候选人的最后日志索引id，这里指的是[]logEntry的最大值
	LastLogTerm  int //与lastLogIndex对应
}
type RequestVoteReply struct {
	Term        int  //响应节点的currentTerm，返回该值的目的是，如果响应节点有更大的currentTerm，那么请求节点要更新
	VoteGranted bool //是否赢得投票
	/*
		接收者拒绝投票的逻辑：
		1. 如果request.term < currentTerm 返回false, 并在reponse.term附上自己的currentTerm
		2. vatedFor != null && request.candidateId != vateFor，实际上就是接收节点已经给其他candidate投票过的情况
		3. request.lastLogIndex <  maxIndex(receiver.log) ，实际上就是接受者的最大日志索引比请求者的大
	*/
}

//2. 新增日志RPC request 和 response
type AppendLogRequest struct {
	Term              int //currentTerm
	LeaderId          int
	PrevLogIndex      int //这里指的是leader.nextIndex[i]
	PreLogTerm        int
	entries           []LogEntry //准备新增的日志
	LeaderCommitIndex int        //Leader 已经提交的最大日志索引, 实际上就是leader.commitIndex
}
type AppendLogResponse struct {
	Term      int
	Success   bool
	NextIndex int
	/*
		接收者接收请求时的处理逻辑：
		1. request.term < receiver.currentTerm 返回 currentTerm, false
		2. request.prevLogIndex等于maxIndex(receiver.log), 但prevLogTerm 不匹配，返回 currentTerm, false
		3. request.prevLogIndex小于maxIndex(receiver.log), 删除prevLogIndex之后所有日志
		4. request.prevLogIndex大于maxIndex(receiver.log), 返回currentTerm, false, maxIndex(receiver.log)  --> Leader收到这个后会重试
		4. entries为空时不做处理
		5. request.leaderCommitIndex > receiver.commitIndex，令 commitIndex 等于 leaderCommit 和 maxIndex(receiver.log) 中较小的一个
	*/
}

//返回 （当前term,  是否leader）
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
