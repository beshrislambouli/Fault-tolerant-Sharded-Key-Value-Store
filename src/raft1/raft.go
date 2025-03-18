package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Entry struct {
	Command interface{}
	Term int
	Index int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	IsLeader bool
	IsCandidate bool
	CurrnetTerm int
	VotedFor int
	
	// NextElectionTimeout time.Time
	LastHeartBeat time.Time

	log []Entry
	applyCh chan raftapi.ApplyMsg

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	
	if rf.CurrnetTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrnetTerm
		return
	}

	if rf.CurrnetTerm < args.Term {
		rf.StartFollower(args.Term)
	}
	

	up_to_date := false;
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		up_to_date = args.LastLogIndex >= rf.log[len(rf.log)-1].Index
	} else {
		up_to_date = args.LastLogTerm >= rf.log[len(rf.log)-1].Term
	}


	if up_to_date && ( rf.VotedFor == -1 || rf.VotedFor == args.CandidateId ){
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.CurrnetTerm
		rf.persist()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrnetTerm
	}
}

type AppendEntriesArgs struct {
	Term int
	Entries []Entry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	Len int
	ConIndex int
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrnetTerm < args.Term {
		rf.StartFollower(args.Term)
	}

	if rf.CurrnetTerm > args.Term {
		reply.Term = rf.CurrnetTerm
		reply.Success = false
		return
	}

	rf.LastHeartBeat = time.Now()

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false;
		reply.Len = len(rf.log)
		return
	} else if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false;
		for e := 0 ; e < len (rf.log) ; e ++ {
			if rf.log[e].Term == rf.log[args.PrevLogIndex].Term {
				reply.ConIndex = rf.log [e].Index
				break
			}
		}
		return
	}
	
	reply.Success = true
	
	for _, entry := range args.Entries {
		if entry.Index >= 0 && entry.Index < len(rf.log) && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		}
	}

	for _, entry := range args.Entries {
		if entry.Index > 0 && len(rf.log) - 1 < entry.Index {
			rf.log = append(rf.log, entry)
		}
	}


	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min (args.LeaderCommit,len(rf.log)-1)
	}
	
	rf.persist()
}

// LEADER

func (rf *Raft) StartLeader() {
	rf.IsLeader = true
	rf.IsCandidate = false
	rf.LastHeartBeat = time.Now()

	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex [server] = len(rf.log)
		rf.MatchIndex[server] = 0
	}
	go rf.heartbeat()
	go rf.commit()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.IsLeader {
		return -1,-1,false
	}

	index := len(rf.log)
	term := rf.CurrnetTerm
	isLeader := true

	rf.log = append (rf.log, Entry{command,term,index})
	rf.persist()

	for server := 0 ; server < len(rf.peers) ; server ++ { 
		if server == rf.me {continue}
		go rf.sendEntries(server)
	}
	
	return index, term, isLeader
}



func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	newCommitIndex := rf.CommitIndex + 1
	for !rf.killed() && rf.IsLeader {
		if len(rf.log) >= newCommitIndex {
			
			NumReplicated  := 1
			for server := 0 ; server < len(rf.peers) ; server ++ {
				if server == rf.me {continue}
				if rf.MatchIndex[server] >= newCommitIndex {
					NumReplicated ++
				}
			}
			if NumReplicated >= len(rf.peers)/2 + 1 && rf.log [newCommitIndex].Term == rf.CurrnetTerm {
				rf.CommitIndex = newCommitIndex
			}
			newCommitIndex ++
		} else {
			newCommitIndex = rf.CommitIndex + 1
		}

		rf.mu.Unlock()
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
	}
}


func (rf *Raft) heartbeat() {
	for server := 0 ; server < len (rf.peers) ; server ++ {
		if server == rf.me {continue}

		go func(server int) {
			rf.mu.Lock()
			for !rf.killed() && rf.IsLeader {
				go rf.sendEntries(server)

				rf.mu.Unlock()
				ms := 100
				time.Sleep(time.Duration(ms) * time.Millisecond)
				rf.mu.Lock()
			}
			rf.mu.Unlock()
		}(server)
	}
}


// FOLLOWER 
func (rf *Raft) StartFollower(nTerm int) { // todo, should i edit the voting?
	
	rf.IsLeader = false
	rf.IsCandidate = false

	rf.CurrnetTerm = nTerm
	rf.VotedFor = -1

	rf.LastHeartBeat = time.Now()
	rf.persist()
}

func (rf *Raft) electionTimeout() {
	for !rf.killed() {
		rand.Seed(time.Now().UnixNano())

		start := time.Now()
		ms := 300 + (rand.Int63() % 2)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.LastHeartBeat.Before(start) && !rf.IsLeader {
			go rf.candidate()
		}
		rf.mu.Unlock()

		ms = 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}



// CANDIDATE 

func (rf *Raft) StartCandidate() { // todo, should i edit the voting?
	rf.IsLeader = false
	rf.IsCandidate = true
	
	rf.CurrnetTerm++
	rf.VotedFor = rf.me

	rf.LastHeartBeat = time.Now()
	rf.persist()
}

func (rf *Raft) candidate() {
	rf.mu.Lock()
	rf.StartCandidate()
	rf.mu.Unlock()


	
	NumVotes := 1
	var once sync.Once
	launch := func() {
		rf.StartLeader()
	}

	for server := 0 ; server < len(rf.peers) ; server ++ {
		if server == rf.me {continue}

		go func(server int) {

			VoteGranted := rf.sendVote(server)
			
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.IsCandidate {return}

			if VoteGranted {
				NumVotes++
				if NumVotes >= len(rf.peers)/2+1 {
					once.Do(launch)
				}
			}
		}(server)
	}
}



// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.CurrnetTerm = 1
	rf.VotedFor = -1
	rf.applyCh = applyCh
	rf.log = append(rf.log, Entry{"$",0,0}) //placeholder to start index from 1 
	rf.CommitIndex = 0
	rf.LastApplied = 0
	
	// just place holders
	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.log))
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}

	// if me == 0 {
	// 	rf.StartLeader()
	// } else {
	// 	rf.StartFollower(1)
	// }
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// DPrintf("CurrentTerm %v votedfor %v log %v",rf.CurrnetTerm,rf.VotedFor,rf.log)

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.applyLog()
	go rf.electionTimeout()

	return rf
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// var term int
	// var isleader bool
	// Your code here (3A).
	return rf.CurrnetTerm, rf.IsLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrnetTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurrnetTerm int
	var VotedFor int
	var log []Entry

	if d.Decode(&CurrnetTerm) != nil ||
	   d.Decode(&VotedFor) != nil || 
	   d.Decode(&log) != nil {
	  panic("ERROR")
	} else {
	  rf.CurrnetTerm = CurrnetTerm
	  rf.VotedFor = VotedFor
	  rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


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
func (rf *Raft) sendVote(server int) bool {
	rf.mu.Lock()

	args := RequestVoteArgs{
		Term: rf.CurrnetTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}

	rf.mu.Unlock()

	rf.sendRequestVote(server,&args,&reply)
	
	return reply.VoteGranted
}

func (rf *Raft) sendEntries(server int) {
	rf.mu.Lock()
	if !rf.IsLeader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term: rf.CurrnetTerm,
		LeaderCommit:rf.CommitIndex,
	}


	if rf.NextIndex[server] - 1 >= 0 {
		args.PrevLogIndex = rf.log[rf.NextIndex[server] - 1].Index
		args.PrevLogTerm  = rf.log[rf.NextIndex[server] - 1].Term
	}

	if rf.NextIndex[server] >= 0 {
		args.Entries = rf.log[rf.NextIndex[server]:]
	}


	reply := AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server,&args,&reply)
	if !ok {return}

	rf.mu.Lock()
	if reply.Term > rf.CurrnetTerm {
		rf.StartFollower(reply.Term)
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.NextIndex[server] = rf.MatchIndex[server] + 1
		rf.mu.Unlock()
	} else {
		if reply.Len > 0 {
			rf.NextIndex[server] = reply.Len
		} else {
			rf.NextIndex[server] = reply.ConIndex
		}

		rf.mu.Unlock()
		rf.sendEntries(server)
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) applyCmd(index int) {
	msg := raftapi.ApplyMsg {
		CommandValid: true,
		Command: rf.log[index].Command,
		CommandIndex: index,
	}
	rf.applyCh <- msg

}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex > rf.LastApplied {
			rf.LastApplied ++ 
			rf.applyCmd(rf.LastApplied)
		}
		rf.mu.Unlock()
		ms := 1
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}