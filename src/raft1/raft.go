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

	CurrentSnapshot []byte
	LastSnapshotIndex int
	LastSnapshotTerm int
	SnapshotLogSz int
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
	LastLogIndex, LastLogTerm := rf.PrevLogEntry(len(rf.log)+rf.SnapshotLogSz)
	if args.LastLogTerm == LastLogTerm {
		up_to_date = args.LastLogIndex >= LastLogIndex
	} else {
		up_to_date = args.LastLogTerm >= LastLogTerm
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

	XTerm int
	XIndex int
	XLen int
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

	if len(rf.log) == 0 {
		if args.PrevLogIndex != rf.LastSnapshotIndex ||
		   args.PrevLogTerm != rf.LastSnapshotTerm {
			reply.Success = false
			return
		}
	} else if args.PrevLogIndex - rf.SnapshotLogSz >= len(rf.log) {
		reply.Success = false;
		reply.XLen = len(rf.log) + rf.SnapshotLogSz
		return
	} else if args.PrevLogIndex - rf.SnapshotLogSz >= 0 && rf.log[args.PrevLogIndex-rf.SnapshotLogSz].Term != args.PrevLogTerm {
		reply.Success = false;
		reply.XTerm = rf.log[args.PrevLogIndex-rf.SnapshotLogSz].Term
		reply.XIndex = -1 
		for e := 0 ; e < len (rf.log) ; e ++ {
			if rf.log[e].Term == rf.log[args.PrevLogIndex-rf.SnapshotLogSz].Term {
				reply.XIndex = rf.log [e].Index // todo ?
				break
			}
		}
		return
	}
	
	reply.Success = true
	
	for _, entry := range args.Entries {
		if entry.Index - rf.SnapshotLogSz >= 0 && entry.Index < len(rf.log) + rf.SnapshotLogSz && rf.log[entry.Index-rf.SnapshotLogSz].Term != entry.Term {
			rf.log = rf.log[:entry.Index-rf.SnapshotLogSz]
		}
	}

	for _, entry := range args.Entries {
		if entry.Index >= 0 && len(rf.log) - 1 + rf.SnapshotLogSz < entry.Index {
			rf.log = append(rf.log, entry)
		}
	}


	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min (args.LeaderCommit,len(rf.log)-1+rf.SnapshotLogSz)
	}
	
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term int
	LastIncludedIndex int 
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrnetTerm > args.Term {
		reply.Term = rf.CurrnetTerm
		return
	}

	if rf.LastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	rf.CurrentSnapshot = args.Data
	rf.LastSnapshotIndex = args.LastIncludedIndex
	rf.LastSnapshotTerm = args.LastIncludedTerm
	rf.SnapshotLogSz = rf.LastSnapshotIndex + 1 
	rf.CommitIndex = args.LastIncludedIndex
	rf.LastApplied = args.LastIncludedIndex
	rf.log = rf.log[len(rf.log):]

	rf.persist()

	msg := raftapi.ApplyMsg {
		SnapshotValid: true,
		Snapshot: rf.CurrentSnapshot,
		SnapshotTerm: rf.LastSnapshotTerm,
		SnapshotIndex: rf.LastSnapshotIndex + 1 ,
	}

	select {
	case rf.applyCh <- msg:
		// Successfully sent message
	default:
		// Channel is closed - ignore the message
	}
}

// LEADER

func (rf *Raft) StartLeader() {
	rf.IsLeader = true
	rf.IsCandidate = false
	rf.LastHeartBeat = time.Now()

	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex [server] = len(rf.log) + rf.SnapshotLogSz
		rf.MatchIndex[server] = -1 + rf.SnapshotLogSz
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

	index := len(rf.log) + rf.SnapshotLogSz
	term := rf.CurrnetTerm
	isLeader := true

	rf.log = append (rf.log, Entry{command,term,index})
	rf.persist()

	for server := 0 ; server < len(rf.peers) ; server ++ { 
		if server == rf.me {continue}
		go rf.sendEntries(server)
	}
	
	return index+1, term, isLeader
}



func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	newCommitIndex := rf.CommitIndex + 1
	for !rf.killed() && rf.IsLeader {
		if len(rf.log) + rf.SnapshotLogSz > newCommitIndex {
			
			NumReplicated  := 1
			for server := 0 ; server < len(rf.peers) ; server ++ {
				if server == rf.me {continue}
				if rf.MatchIndex[server] >= newCommitIndex {
					NumReplicated ++
				}
			}
			if NumReplicated >= len(rf.peers)/2 + 1 && rf.log [newCommitIndex-rf.SnapshotLogSz].Term == rf.CurrnetTerm {
				rf.CommitIndex = newCommitIndex
			}
			newCommitIndex ++
		} else {
			newCommitIndex = rf.CommitIndex + 1
		}

		rf.mu.Unlock()
		ms := 5
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
func (rf *Raft) StartFollower(nTerm int) {
	
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

func (rf *Raft) StartCandidate() {
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
		rf.mu.Unlock()
		rf.Start("PUSH")
		rf.mu.Lock()
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
	// rf.log = append(rf.log, Entry{"$",0,0}) //placeholder to start index from 1 
	rf.LastSnapshotIndex = -1
	rf.LastSnapshotTerm  = -1
	rf.SnapshotLogSz = 0

	rf.CommitIndex = -1
	rf.LastApplied = -1
	
	// just place holders
	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.log) + rf.SnapshotLogSz)
		rf.MatchIndex = append(rf.MatchIndex, -1 + rf.SnapshotLogSz)
	}

	// if me == 0 {
	// 	rf.StartLeader()
	// } else {
	// 	rf.StartFollower(1)
	// }
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
	e.Encode(rf.LastSnapshotIndex)
	e.Encode(rf.LastSnapshotTerm)
	e.Encode(rf.SnapshotLogSz)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.CurrentSnapshot)
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
	var LastSnapshotIndex int
	var LastSnapshotTerm int
	var SnapshotLogSz int

	if d.Decode(&CurrnetTerm) != nil ||
	   d.Decode(&VotedFor) != nil || 
	   d.Decode(&log) != nil || 
	   d.Decode(&LastSnapshotIndex) != nil ||
	   d.Decode(&LastSnapshotTerm) != nil || 
	   d.Decode(&SnapshotLogSz) != nil {
	  panic("ERROR")
	} else {
	  rf.CurrnetTerm = CurrnetTerm
	  rf.VotedFor = VotedFor
	  rf.log = log
	  rf.LastSnapshotIndex = LastSnapshotIndex
	  rf.LastSnapshotTerm = LastSnapshotTerm
	  rf.SnapshotLogSz = SnapshotLogSz
	  rf.CommitIndex = rf.LastSnapshotIndex
	  rf.LastApplied = rf.LastSnapshotIndex
	}
	rf.CurrentSnapshot = rf.persister.ReadSnapshot()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index -- 

	if index >= len(rf.log) + rf.SnapshotLogSz || index - rf.SnapshotLogSz < 0 {
		return
		panic("ERROR Snapshot")
	}
	
	rf.CurrentSnapshot = snapshot
	rf.LastSnapshotTerm = rf.log[index - rf.SnapshotLogSz].Term
	rf.log = rf.log [index + 1  - rf.SnapshotLogSz:]
	rf.SnapshotLogSz = index + 1 
	rf.LastSnapshotIndex = index


	rf.persist()
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
		// LastLogIndex: rf.log[len(rf.log)-1].Index,
		// LastLogTerm: rf.log[len(rf.log)-1].Term,
		LastLogIndex: rf.LastSnapshotIndex,
		LastLogTerm: rf.LastSnapshotTerm,
	}
	args.LastLogIndex, args.LastLogTerm = rf.PrevLogEntry(len(rf.log)+rf.SnapshotLogSz)

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
		LeaderCommit: rf.CommitIndex,
		PrevLogIndex: rf.LastSnapshotIndex,
		PrevLogTerm: rf.LastSnapshotTerm,
	}

	if rf.NextIndex[server] - rf.SnapshotLogSz - 1 < 0 {
		args.PrevLogIndex = rf.LastSnapshotIndex
		args.PrevLogTerm = rf.LastSnapshotTerm
	} else if rf.NextIndex[server] - 1 - rf.SnapshotLogSz < len(rf.log) && rf.NextIndex[server] - 1 - rf.SnapshotLogSz >= 0 {
		// args.PrevLogIndex = rf.log[rf.NextIndex[server] - 1].Index
		// args.PrevLogTerm  = rf.log[rf.NextIndex[server] - 1].Term
		args.PrevLogIndex, args.PrevLogTerm = rf.PrevLogEntry(rf.NextIndex[server])
	}

	if rf.NextIndex[server] < len(rf.log) + rf.SnapshotLogSz && rf.NextIndex[server] - rf.SnapshotLogSz >= 0 {
		args.Entries = rf.log[rf.NextIndex[server]-rf.SnapshotLogSz:]
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
		if reply.XLen > 0 {
			if reply.XLen <= rf.LastSnapshotIndex {
				rf.sendSnapshot(server)
			}
			rf.NextIndex[server] = reply.XLen
		} else {

			IndexForXTerm := -1 
			for e := len(rf.log) - 1 ; e >= 0 ; e -- {
				if rf.log[e].Term == reply.XTerm {
					IndexForXTerm = rf.log[e].Index
				}
			}

			if IndexForXTerm == -1 { // leader doesn't have XTerm

				if reply.XIndex == -1 {
					rf.sendSnapshot(server)
				}
				
				rf.NextIndex[server] = reply.XIndex
			} else { // leader has XTerm
				rf.NextIndex[server] = IndexForXTerm + 1 
			}
			
		}

		rf.NextIndex[server] = max(rf.NextIndex[server],rf.SnapshotLogSz)

		rf.mu.Unlock()
		rf.sendEntries(server)
	}
}

func (rf *Raft) sendSnapshot(server int) {
	args := InstallSnapshotArgs {
		Term: rf.CurrnetTerm,
		LastIncludedIndex: rf.LastSnapshotIndex,
		LastIncludedTerm: rf.LastSnapshotTerm,
		Data: rf.CurrentSnapshot,
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	rf.sendInstallSnapshot(server,&args,&reply)

	rf.mu.Lock()
	if reply.Term > rf.CurrnetTerm {
		rf.StartFollower(reply.Term)
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close (rf.applyCh)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) applyCmd(index int) {
	msg := raftapi.ApplyMsg {
		CommandValid: true,
		Command: rf.log[index-rf.SnapshotLogSz].Command,
		CommandIndex: index+1,
	}
	// rf.mu.Unlock()
	select {
	case rf.applyCh <- msg:
		// Successfully sent message
	default:
		// Channel is closed - ignore the message
	}

}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex > rf.LastApplied && len(rf.log) + rf.SnapshotLogSz > rf.CommitIndex {
			rf.LastApplied ++ 
			rf.applyCmd(rf.LastApplied)
		}

		rf.mu.Unlock()
		ms := 1
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	
}

func (rf *Raft) PrevLogEntry(index int) (int, int) {
	if index - rf.SnapshotLogSz > len(rf.log) || index - rf.SnapshotLogSz < 0 {
		panic ("ERROR PrevLogEntry")
	}
	if index == rf.SnapshotLogSz {
		return rf.LastSnapshotIndex, rf.LastSnapshotTerm
	}
	return rf.log[index-1-rf.SnapshotLogSz].Index, rf.log[index-1-rf.SnapshotLogSz].Term
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
