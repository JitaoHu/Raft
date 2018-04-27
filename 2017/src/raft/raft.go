package raft

//一个服务通过调用Make(peers,me,…)创建一个Raft端点。peers参数是通往其他Raft端点处于连接状态下的RPC连接。
//me参数是自己在端点数组中的索引。

//Start(command)要求Raft开始将command命令追加到日志备份中。Start()函数马上返回，不等待处理完成。
//服务期待你们的实现发生一个ApplyMsg结构给每个完全提交的日志，通过applyCh通道。
//使用可持久化的对象(查看persister.go)保存和恢复状态
//使用ReadRaftState()和SaveRaftState() 方法分别处理读取和存储的操作。
import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	Candidate = 0
	Follower  = 1
	Leader    = 2
)

const (
	Heartbeat = 100 * time.Millisecond //心跳
	Timeout   = 150                    //超时
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state int // role of rf
	//定义所有服务器的持久化状态
	currentTerm       int        // 服务器看到的最后一个任期
	votedForCandidate int        // 当前任期收到的candidate
	votedForTerm      int        // 收到的candidate的任期，当votedForTerm != currentTerm
	log               []LogEntry // log storage for index, state machine already stored elsewhere

	// 所有服务器上的不稳定的状态
	commitIndex int // 已提交的日志条目的最高索引值
	lastApplied int // 应用到状态机的日志条目的最高索引值

	// 领导者的不稳定状态
	nextIndex  []int //对于每一个服务器，记录着下一个要发送的日志条目索引值
	matchIndex []int //对于每一个服务器，已知被复制到该服务器的最高日志条目索引值

	// other necessary vars
	lastReceived int64 // last time receive rpc from others

	applyCh chan ApplyMsg
}

//获取节点状态
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here.
	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

//进行编码存储，以便网络恢复后读取状态。
func (rf *Raft) persist() {
	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(&rf.currentTerm)
	e.Encode(&rf.votedForCandidate)
	e.Encode(&rf.votedForTerm)
	e.Encode(&rf.log)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
}

//解码并恢复状态。
func (rf *Raft) readPersist(data []byte) {
	// Your code here.

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedForCandidate)
	d.Decode(&rf.votedForTerm)
	d.Decode(&rf.log)
}

type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate的term
	CandidateId  int //candidate请求投票
	LastLogIndex int //candidate最后日志条目的索引
	LastLogTerm  int //candidate最后日志条目的term
}

type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm,for candidate to update itself
	VoteGranted bool //true表示候选者获得投票
}

func LastIndex(log []LogEntry) int {
	return log[len(log)-1].Index
}

func lastTerm(log []LogEntry) int {
	return log[len(log)-1].Term
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//服务器term大于candidate的term，返回false并更新term
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//term条件满足，转换状态
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	uptodate := false
	// from 5.4
	//如果candidate日志至少与这些服务器中的任何一台一样新（uptodate），
	//那么他就持有了所有那些已提交的条目
	if lastTerm(rf.log) < args.LastLogTerm {
		uptodate = true
	}
	if lastTerm(rf.log) == args.LastLogTerm && LastIndex(rf.log) <= args.LastLogIndex {
		uptodate = true
	}

	// 5.2 based on 5.4 uptodate flag
	//如果该leaderterm比此candidate大，或者一样大，且uptodate
	granted := false
	if (rf.votedForTerm < args.Term || rf.votedForCandidate == args.CandidateId) && uptodate {
		granted = true
	}
	//如果投票了，更新自身参数
	if granted {
		rf.votedForTerm = args.Term // rf would not have higher term than master if granted vote
		rf.votedForCandidate = args.CandidateId
		rf.lastReceived = time.Now().UnixNano()
	}

	// write to stable store before respond
	//把状态更新到持久化存储上
	rf.persist()

	reply.VoteGranted = granted
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader的term
	LeaderId     int        //follower可以通过该ID为客户端进行重定向
	PrevLogIndex int        //新条目的前一个日志条目索引（用于一致性检查）
	PrevLogTerm  int        //新条目的千一个日志条目索引的term（用于一致性检查）
	Entries      []LogEntry //用于保存的日志条目（若为空则为心跳消息，可能为了高效而一次发送多条记录）
	LeaderCommit int        // leader commit index
}

type AppendEntriesReply struct {
	Term      int  //current,用于leader更新
	Success   bool //follower如果包括日志条目中匹配prevLogIndex，prevLogTerm则为true
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// reply from server to candidate

	// 5.1
	//服务器之间通讯term，如果leader term小则false并更新
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = -1
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 5.2 else 5.1
	//如果收到从新leader发送的AppendEntries RPC请求，转换成follower
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.lastReceived = time.Now().UnixNano()

	// 5.3
	// len / LastIndex critical
	//当发送一个RPC，leader在紧接着新条目的后面包含着前一个条目的index和term（prevLogIndex，prevLogTerm）
	//如果follower在他的日志中找不到该index和term，那么他拒绝新条目
	if args.PrevLogIndex >= len(rf.log) {
		rf.persist()
		reply.Success = false
		reply.NextIndex = len(rf.log)
		return
	}

	// optimized necessary speed requirement
	//leader强制follower日志与自己一致
	//通过递减nextIndex找到与自己相同的条目，删除之后的所有条目，将自己的复制给follower
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		nextIndex := args.PrevLogIndex

		for {
			if rf.log[nextIndex].Term != rf.log[args.PrevLogIndex].Term {
				break
			}
			nextIndex--
		}

		rf.persist()

		reply.Success = false
		reply.NextIndex = nextIndex + 1
		return
	}

	reply.Success = true

	// write
	// conflict solution
	// dump / save log to persist store each time commit
	rf.log = rf.log[0:(args.PrevLogIndex + 1)]

	for i := 0; i < len(args.Entries); i++ {
		if args.Entries[i].Index > LastIndex(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

	// update commitIndex and ApplyMsg  5.3
	//follower通过leader发送来的appendrntries请求，发现LC>commitIndex,更新commitIndex=leaderCommit
	if args.LeaderCommit > rf.commitIndex {
		newcommitIdx := args.LeaderCommit
		//接着判断如果commitIndex>lastApplied则将commitIndex日志条目应用到状态机
		if LastIndex(rf.log) < newcommitIdx {
			newcommitIdx = LastIndex(rf.log)
		}
		rf.commitIndex = newcommitIdx

		for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
			msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.applyCh <- msg
		}
		rf.lastApplied = rf.commitIndex
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendHeartbeats() {
	for {

		rf.mu.Lock()
		li := LastIndex(rf.log)
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if !isLeader {
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(index int, peer int) {
					//心跳（不包含日志条目的appendEntries RPCs） from leader
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderCommit: rf.commitIndex, LeaderId: rf.me}
					args.PrevLogIndex = rf.nextIndex[peer] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

					for i := rf.nextIndex[peer]; i < index+1; i++ {
						args.Entries = append(args.Entries, rf.log[i])
					}

					reply := AppendEntriesReply{}

					ok := rf.sendAppendEntries(peer, args, &reply)

					if !ok {
						return
					}
					//term条件不符，转状态
					if reply.NextIndex == -1 {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
						}

						rf.state = Follower
						rf.persist()

						rf.mu.Unlock()
						return
					}

					rf.mu.Lock()
					//心跳成功，更新状态
					if reply.Success {
						rf.nextIndex[peer] = index + 1
						rf.matchIndex[peer] = index
					} else {
						rf.nextIndex[peer] = reply.NextIndex
					}

					rf.mu.Unlock()
				}(li, i)
			}
		}

		rf.mu.Lock()

		//更新commitIndex  5.3
		newcommitIdx := rf.commitIndex
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			n := 1
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
					n++
				}
			}
			//收到多数的回复就将命令应用到状态机（更新commlitIndex）
			if 2*n > len(rf.peers) {
				newcommitIdx = i
			}
		}

		for i := rf.commitIndex + 1; i < newcommitIdx+1; i++ {
			msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.applyCh <- msg
		}

		rf.commitIndex = newcommitIdx

		rf.mu.Unlock()

		time.Sleep(Heartbeat)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	index = LastIndex(rf.log) + 1
	entry := LogEntry{Command: command, Index: index, Term: term}
	rf.log = append(rf.log, entry)

	rf.persist()

	return index, term, isLeader
}

func (rf *Raft) Kill() {

}
func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Election() {
	rf.mu.Lock()

	// 增加term
	rf.currentTerm++
	rf.state = Candidate //转变成候选人

	rf.mu.Unlock()

	// 每个服务器有1票
	nvotes := 1
	voteChan := make(chan RequestVoteReply, len(rf.peers))

	// send RequestVoteArgs
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: LastIndex(rf.log), LastLogTerm: lastTerm(rf.log)}
				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(peer, args, &reply)
				if !ok {
					reply.VoteGranted = false
				}

				voteChan <- reply

			}(i)
		}
	}

	for i := 0; i < len(rf.peers)/2; i++ {
		v := <-voteChan
		if v.VoteGranted {
			nvotes++
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//有脑裂情况发生
	if rf.state == Follower {
		return
	}

	if 2*nvotes > len(rf.peers) {
		rf.state = Leader

		for i := 0; i < len(rf.peers); i++ {
			// reinit after election each time
			if i != rf.me {
				rf.nextIndex[i] = LastIndex(rf.log) + 1
				rf.matchIndex[i] = 0
			}
		}
		// heartbeats to all
		go rf.SendHeartbeats()
	}
}
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	lastIndex := LastIndex(rf.log)

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-baseIndex].Term})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)

}

//初始化Raft节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0 //初始化为零，单调递增
	rf.votedForCandidate = -1
	rf.votedForTerm = 0
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0}) // last valid entry idx = 1

	//
	rf.commitIndex = 0
	rf.lastApplied = 0

	//
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = Follower
	rf.lastReceived = time.Now().UnixNano()

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// go routine for start voting, candidate state
	go func() {
		for {

			electionTimeout := int64(1e6 * (Timeout + rand.Intn(Timeout)))
			time.Sleep(time.Duration(electionTimeout))

			now := time.Now().UnixNano()

			rf.mu.Lock()
			reqElection := (now-rf.lastReceived) >= electionTimeout && rf.state != Leader
			rf.mu.Unlock()

			if reqElection {
				go rf.Election()
			}
		}
	}()

	return rf
}
