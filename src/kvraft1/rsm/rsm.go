package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	muQuery		 sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	resCh        chan any

	index int
	term int
	isLeader bool

	LastAppliedIndex int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		resCh:        make(chan any),
		LastAppliedIndex: -1,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	
	raft.DPrintf("RSM %v just started",rsm.me)

	data := persister.ReadSnapshot()
	if len (data) > 0 {
		raft.DPrintf("RSM %v restore after rebooting",rsm.me)
		rsm.sm.Restore(data)
	}

	go rsm.Reader()
	go rsm.CheckSnapshot()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	
	rsm.muQuery.Lock()
	defer rsm.muQuery.Unlock()

	rsm.mu.Lock()
	raft.DPrintf("RSM %v - Query Start: %v",rsm.me,req)
	rsm.index, rsm.term, rsm.isLeader = rsm.rf.Start(req)
	raft.DPrintf("RSM %v - Query Start Res: index: %v term: %v isLeader: %v",rsm.me,rsm.index,rsm.term,rsm.isLeader)
	if !rsm.isLeader {rsm.mu.Unlock(); return rpc.ErrWrongLeader, nil}
	rsm.mu.Unlock()
	raft.DPrintf("RSM %v is waiting for args: %v",rsm.me, req)
	res, ok := <- rsm.resCh
	raft.DPrintf("RSM %v, Done waiting, gonna send: %v %v",rsm.me,res,ok)
	if !ok {return rpc.ErrWrongGroup, nil}
	if res == "ERROR" {return rpc.ErrWrongLeader, nil}

	return rpc.OK, res 
}

func (rsm *RSM) Reader() {
	for replicated := range rsm.applyCh {
		raft.DPrintf("Reader %v - Command: %v",rsm.me,replicated)

		if replicated.Command == "PUSH" {continue}
		
		if replicated.SnapshotValid {
			raft.DPrintf("Reader %v INSTALLING SNAPSHOT with this index %v",rsm.me,replicated.SnapshotIndex)
			rsm.mu.Lock()
			rsm.sm.Restore(replicated.Snapshot)
			rsm.LastAppliedIndex = replicated.SnapshotIndex
			rsm.mu.Unlock()
			continue
		}

		rsm.mu.Lock()

		res := rsm.sm.DoOp(replicated.Command)
		rsm.LastAppliedIndex = replicated.CommandIndex
		

		curIndex := replicated.CommandIndex
		curTerm, curIsLeader := rsm.rf.GetState()

		raft.DPrintf("Reader %v - RSM_State: index: %v term: %v isLeader: %v",rsm.me,rsm.index,rsm.term,rsm.isLeader)
		raft.DPrintf("Reader %v - CUR_State: index: %v term: %v isLeader: %v",rsm.me,curIndex,curTerm,curIsLeader)
		if curIndex < rsm.index {rsm.mu.Unlock(); continue}
		if !rsm.isLeader {rsm.mu.Unlock(); continue}

		if rsm.term != curTerm || !curIsLeader { 
			rsm.mu.Unlock()
			rsm.resCh <- "ERROR"
		} else {
			rsm.mu.Unlock()
			rsm.resCh <- res
		}
	}
	raft.DPrintf("Reader %v Closing Channel",rsm.me)
	close(rsm.resCh)
}


func (rsm *RSM) CheckSnapshot() {

	for rsm.maxraftstate != -1 { // TODO: should check killed??

		if rsm.rf.PersistBytes() > rsm.maxraftstate {
			raft.DPrintf("RSM %v too big -> snapshot",rsm.me)
			rsm.rf.Snapshot(rsm.LastAppliedIndex,rsm.sm.Snapshot())
		}

		time.Sleep(100 * time.Millisecond)
	}
}