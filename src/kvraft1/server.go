package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/tester1"
)

type Value struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	Store map[string]Value
	mu sync.Mutex
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.DoGet(args)
	case rpc.PutArgs:
		return kv.DoPut(args)
	default:
		panic("Not Known Request")
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	raft.DPrintf("KVSERVER %v SNAPSHOT",kv.me)

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(kv.Store)

	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	raft.DPrintf("KVSERVER %v RESTORE",kv.me)

	
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)

	var Store map[string]Value

	if decoder.Decode(&Store) != nil {
		panic ("ERROR DECODING STATE")
	} else {
		kv.Store = Store
	}
}

func (kv *KVServer) DoGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := rpc.GetReply{}

	if val, ok := kv.Store[args.Key]; ok {
		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK
 	} else 	{
		reply.Err = rpc.ErrNoKey
	}
	
	return reply
}

func (kv *KVServer) DoPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	reply := rpc.PutReply{}

	if val, ok := kv.Store[args.Key]; ok {
		if val.Version == args.Version {
			kv.Store[args.Key] = Value{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.Store[args.Key] = Value{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

	return reply
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err 
		return
	}
	*reply = res.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	raft.DPrintf("KVSERVER %v got killed",kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,
		Store: make(map[string]Value),
	}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
