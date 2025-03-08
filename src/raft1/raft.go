package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)


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

	log []interface{}
	logTerm []int
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
	reply.Term = rf.CurrnetTerm

	if rf.CurrnetTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	rf.checkTerm(args.Term)

	up_to_date := false;
	if args.LastLogTerm > rf.logTerm[len(rf.logTerm)-1] {
		up_to_date = true
	} else if args.LastLogTerm == rf.logTerm[len(rf.logTerm)-1] && args.LastLogIndex >= len(rf.log)-1 {
		up_to_date = true
	}
	

	if up_to_date && ( rf.VotedFor == -1 || rf.VotedFor == args.CandidateId ){
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term int
	Entries []interface{}
	EntriesTerm []int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.CurrnetTerm
	rf.LastHeartBeat = time.Now()

	if rf.CurrnetTerm > args.Term {
		reply.Success = false
		return
	} 

	rf.checkTerm(args.Term)
	
	if (len(rf.logTerm) <= args.PrevLogIndex) || ( rf.logTerm[args.PrevLogIndex] != args.PrevLogTerm ) {
		reply.Success = false;
		return
	}
	
	reply.Success = true
	
	if (len(args.Entries) > 0) && (len(rf.logTerm) > args.PrevLogIndex+1) && (rf.logTerm[args.PrevLogIndex+1] != args.EntriesTerm[0]) {
		for len(rf.logTerm) > args.PrevLogIndex+1 {
			rf.log = rf.log[:len(rf.log)-1]
			rf.logTerm = rf.logTerm[:len(rf.logTerm)-1]
		}
	}


	if (len(args.Entries) > 0) && (len(rf.logTerm) == args.PrevLogIndex+1) {
		rf.log = append (rf.log,args.Entries[0])
		rf.logTerm = append(rf.logTerm, args.EntriesTerm[0])
	}


	if args.LeaderCommit > rf.CommitIndex {
		rf.updateCommitIndex( min (args.LeaderCommit,len(rf.log)-1) )
	}
	
}




// LEADER

func (rf *Raft) StartLeader() {
	rf.IsLeader = true
	rf.IsCandidate = false
	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex [server] = len(rf.log)
		rf.MatchIndex[server] = 0
	}
	go rf.heartbeat()
	go rf.commit()
	for server := 0 ; server < len(rf.peers) ; server ++ {
		if server == rf.me {continue}
		go rf.replicate(server)
	}
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
	if !rf.IsLeader {
		return -1,-1,false
	}

	index := len(rf.log)
	term := rf.CurrnetTerm
	isLeader := true
	rf.log = append (rf.log,command)
	rf.logTerm = append (rf.logTerm,rf.CurrnetTerm)
	return index, term, isLeader
}



func (rf *Raft) commit() {
	for rf.killed() == false && rf.IsLeader {
		if len(rf.log) > rf.CommitIndex + 1 {
			newCommitIndex := rf.CommitIndex + 1
			NumReplicated  := 1
			for server := 0 ; server < len(rf.peers) ; server ++ {
				if server == rf.me {continue}
				if rf.MatchIndex[server] >= newCommitIndex {
					NumReplicated ++
				}
			}
			if NumReplicated >= len(rf.peers)/2 + 1 && rf.logTerm [newCommitIndex] == rf.CurrnetTerm {
				rf.updateCommitIndex(newCommitIndex)
			}
		}
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}


func (rf *Raft) replicate(server int) {
	for !rf.killed() && rf.IsLeader {
		if len(rf.log) - 1 >= rf.NextIndex[server] {
			args := AppendEntriesArgs {
				Term: rf.CurrnetTerm,
				Entries: []interface{}{rf.log[rf.NextIndex[server]]},
				EntriesTerm: []int{rf.logTerm[rf.NextIndex[server]]},
				LeaderCommit: rf.CommitIndex,
				PrevLogIndex: rf.NextIndex[server]-1,
				PrevLogTerm: rf.logTerm[rf.NextIndex[server]-1],
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server,&args,&reply)
			if !ok {
				ms := 10
				time.Sleep(time.Duration(ms) * time.Millisecond)
				continue
			}
			if rf.checkTerm(reply.Term) < 0 {return}
			if reply.Success {
				rf.MatchIndex [server] = rf.NextIndex[server]
				rf.NextIndex  [server] ++
				continue
			} else {
				rf.NextIndex [server] -- 
				if rf.NextIndex [server] == 0 {panic("SHOULD NOT FALL BELOW 1")}
				continue
			}
		}
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() && rf.IsLeader {
		for server := 0 ; server < len (rf.peers) ; server ++ {
			if server == rf.me {continue}

			go func(server int) {
				args := AppendEntriesArgs {
					Term: rf.CurrnetTerm,
					PrevLogIndex: len(rf.log)-1, // TODO should we send this or nextindex?
					PrevLogTerm: rf.logTerm[len(rf.log)-1],
					// Entries: empty,
					// EntriesTerm: empty,
					LeaderCommit: rf.CommitIndex,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server,&args,&reply);
				if !ok {return}
				if rf.checkTerm(reply.Term) < 0 {return}
			}(server)

			ms := 100
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}


// FOLLOWER 
func (rf *Raft) StartFollower() { // todo, should i edit the voting?
	rf.IsLeader = false
	rf.IsCandidate = false
	go rf.electionTimeout()
}

func (rf *Raft) electionTimeout() {
	for  {
		start := time.Now()
		ms := 1500 + (rand.Int63() % 1500) // TODO think about this numbers again and maybe consider moving this to top to sleep once and then have a clean for loop
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.killed() || rf.IsLeader {break}
		DPrintf("Server: %v Start: %v lastheartbeat: %v", rf.me,start,rf.LastHeartBeat)
		if rf.LastHeartBeat.After(start) {continue}

		
		rf.StartCandidate()
		return
	}
}



// CANDIDATE 

func (rf *Raft) StartCandidate() { // todo, should i edit the voting?
	rf.IsLeader = false
	rf.IsCandidate = true
	go rf.candidate()
}

func (rf *Raft) candidate() {
	rf.updateTerm(rf.CurrnetTerm+1)
	rf.VotedFor = rf.me
	rf.LastHeartBeat = time.Now()
	NumVotes := 1
	for server := 0 ; server < len(rf.peers) ; server ++ {
		if server == rf.me {continue}

		go func(server int) {

			args := RequestVoteArgs {
				Term: rf.CurrnetTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.log)-1,
				LastLogTerm: rf.logTerm[len(rf.log)-1],
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server,&args,&reply)
			if !ok {return}

			if rf.checkTerm(reply.Term) < 0 {
				return 
			}
			
			if reply.VoteGranted {
				NumVotes++
				if NumVotes >= len(rf.peers)/2+1 {
					rf.StartLeader()
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
	rf.IsLeader = me == 0
	rf.CurrnetTerm = 1
	rf.VotedFor = -1
	rf.applyCh = applyCh
	rf.log = append(rf.log, "$") //placeholder to start index from 1 
	rf.logTerm = append (rf.logTerm,0)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	
	// just place holders
	for server := 0 ; server < len(rf.peers) ; server ++ {
		rf.NextIndex = append(rf.NextIndex, 0)
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}

	if rf.IsLeader {
		rf.StartLeader()
	} else {
		rf.StartFollower()
	}
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()


	return rf
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// HELPERS
func (rf *Raft) updateTerm(newTerm int) {
	rf.CurrnetTerm = newTerm
	rf.VotedFor = -1
}

func (rf *Raft) updateCommitIndex(newCommitIndex int) {
	rf.CommitIndex = newCommitIndex
	rf.applyLog()
}
func (rf *Raft) checkTerm(rpcTerm int) int {
	if rpcTerm > rf.CurrnetTerm {
		rf.updateTerm(rpcTerm)
		if rf.IsLeader || rf.IsCandidate {
			rf.StartFollower()
		}
		return -1;
	}
	return 0;
}


func (rf *Raft) applyCmd(index int) {
	msg := raftapi.ApplyMsg {
		CommandValid: true,
		Command: rf.log[index],
		CommandIndex: index,
	}
	rf.applyCh <- msg

}

func (rf *Raft) applyLog() {
	for rf.CommitIndex > rf.LastApplied {
		rf.LastApplied ++ 
		rf.applyCmd(rf.LastApplied)
	}
}