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

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

const ROLE_LEADER     = "Leader"
const ROLE_FOLLOWER   = "Follower"
const ROLE_CANDIDATES = "Candidates"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int         // 服务器当前的任期
	votedFor    int         // 当前任期内收到选票的候选者id
	log         []*LogEntry // 日志条目
	
	commitIndex int // 已知已提交的最高的日志条目的索引
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引

	nextIndex   []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex  []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引

	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时间：收到leader心跳、请求其他节点投票、给其他人投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号
	VoteGranted bool // 投票情况
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ", rf.me, args.CandidateId, reply.VoteGranted)
	}()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.LastLogTerm < lastLogTerm || args.LastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastActiveTime = time.Now()
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
 
	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)
 
	reply.Term = rf.currentTerm
	reply.Success = false
 
	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, rf.log)
	}()
 
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = args.LeaderId
		rf.persist()
	}
 
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
 
	if len(rf.log) < args.PrevLogIndex {
		return
	}
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
		return
	}
 
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else {
			if rf.log[index - 1].Term != logEntry.Term {
				rf.log = rf.log[:index - 1]
				rf.log = append(rf.log, logEntry)
			}
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}

	logEntry := LogEntry{
		Command: command,
		Term: rf.currentTerm,
	}
	rf.log = append(rf.log, &logEntry)
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration((200+rand.Int31n(150))*int32(time.Millisecond))
			elapses := now.Sub(rf.lastActiveTime)
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}

			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = now

				rf.currentTerm += 1
				rf.votedFor = rf.me

				args := RequestVoteArgs {
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: len(rf.log),
				}

				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)
				
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteCount := 1
				finishCount := 1
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}

						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				if rf.role != ROLE_CANDIDATES {
					return
				}
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					return
				}
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me
					rf.lastBroadcastTime = time.Unix(0, 0)
					return
				}
			}
		}()

	}
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != ROLE_LEADER {
				return
			}

			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			type AppendResult struct {
				peerId int
				resp   *AppendEntriesReply
			}

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]*LogEntry, 0)
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
				}
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerId]-1:]...)
				DPrintf("RaftNode[%d] appendEntries starts, currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, len(rf.log), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)
				
				go func(id int, args1 *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer func() {
							DPrintf("RaftNode[%d] appendEntries ends, currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
								rf.me, rf.currentTerm, id, len(rf.log), rf.nextIndex[id], rf.matchIndex[id], rf.commitIndex)
						}()
						if rf.currentTerm != args1.Term {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.role = ROLE_FOLLOWER
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							return
						}
						if reply.Success {
							rf.nextIndex[id] += len(args1.Entries)
							rf.matchIndex[id] = rf.nextIndex[id] - 1
							sortedMatchIndex := make([]int, 0)
							sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
							}
							sort.Ints(sortedMatchIndex)
							newCommitIndex := sortedMatchIndex[len(rf.peers) / 2]
							if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex - 1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
						}
					} else {
						rf.nextIndex[id] -= -1
						if rf.nextIndex[id] < 1 {
							rf.nextIndex[id] = 1
						}
					}
				}(peerId, &args)
			}
		}()
	}	
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		var appliedMsgs = make([]ApplyMsg, 0)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command: rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				})
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}()

		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
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
	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()              // election逻辑
	go rf.appendEntriesLoop()   // leader逻辑
	go rf.applyLogLoop(applyCh) // apply逻辑

	DPrintf("RaftNode[%d]start", me)

	return rf
}
