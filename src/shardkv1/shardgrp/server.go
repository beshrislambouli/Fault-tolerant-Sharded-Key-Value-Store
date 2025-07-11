package shardgrp

import (
	"bytes"
	// "log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	// kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	gid  tester.Tgid

	// Your code here
	Store map[string]Value
	mu sync.Mutex

	FrozenShards 			map[shardcfg.Tshid]bool
	NumCFG_FreezeShard 		map[shardcfg.Tshid]shardcfg.Tnum
	NumCFG_InstallShard 	map[shardcfg.Tshid]shardcfg.Tnum
	NumCFG_DeleteShard 		map[shardcfg.Tshid]shardcfg.Tnum
}


func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.DoGet(args)
	case rpc.PutArgs:
		return kv.DoPut(args)
	case shardrpc.FreezeShardArgs:
		return kv.DoFreezeShard(args)
	case shardrpc.InstallShardArgs:
		return kv.DoInstallShard(args)
	case shardrpc.DeleteShardArgs:
		return kv.DoDeleteShard(args)
	default:
		panic("Not Known Request")
	}
}


func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()


	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(kv.Store)

	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
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

	if kv.FrozenShards[shardcfg.Key2Shard(args.Key)] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

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

	if kv.FrozenShards[shardcfg.Key2Shard(args.Key)] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

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

func (kv *KVServer) DoFreezeShard(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := shardrpc.FreezeShardReply{}
	

	t := true
	if args.Num > kv.NumCFG_FreezeShard[args.Shard] {
		kv.NumCFG_FreezeShard[args.Shard] = args.Num
	} else {
		t = false
		reply.Err = rpc.ErrWrongGroup
	}


	// add the shard to the frozen shard to reject put/get on it
	if t { kv.FrozenShards[args.Shard] = true}

	// collect the data relevent to this shard
	Store_Shard := make(map[string]Value)
	for key, value := range kv.Store {
		if shardcfg.Key2Shard(key) == args.Shard {
			Store_Shard[key] = value
		}
	}

	// code the data
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(Store_Shard)

	// reply the data
	reply.State = buf.Bytes()

	if t { reply.Err = rpc.OK }
	return reply
}

func (kv *KVServer) DoInstallShard(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := shardrpc.InstallShardReply{}

	if args.Num > kv.NumCFG_InstallShard[args.Shard] {
		kv.NumCFG_InstallShard[args.Shard] = args.Num
	} else {
		// log.Printf("ERROR DoInstallShard")
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// delete the shard to the frozen shards if it was there to accept put/get on it
	kv.FrozenShards[args.Shard] = false

	// decode the data
	buf := bytes.NewBuffer(args.State)
	decoder := labgob.NewDecoder(buf)
	var Store_Shard map[string]Value
	decoder.Decode(&Store_Shard) 
	
	// install the data
	for key, value := range Store_Shard {
		kv.Store [key] = value
	}
	
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) DoDeleteShard(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := shardrpc.DeleteShardReply{}
	
	if args.Num > kv.NumCFG_DeleteShard[args.Shard] {
		kv.NumCFG_DeleteShard[args.Shard] = args.Num
	} else {
		// log.Printf("ERROR DoDeleteShard")
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// delete the shard to the frozen shards if it was there to accept put/get on it
	// kv.FrozenShards[args.Shard] = false

	// collect the data relevent to this shard
	var keys_to_delete []string
	for key := range kv.Store {
		if shardcfg.Key2Shard(key) == args.Shard {
			keys_to_delete = append(keys_to_delete,key)
		}
	}

	// delete the data relevent to this shard from the store
	for _, key := range keys_to_delete {
		delete(kv.Store, key)
	}


	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code hereerr, res := kv.rsm.Submit(*args)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err 
		return
	}
	*reply = res.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.DeleteShardReply)
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
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid: gid, 
		me: me,
		Store: make(map[string]Value),
		FrozenShards: make(map[shardcfg.Tshid]bool),
		NumCFG_FreezeShard : make(map[shardcfg.Tshid]shardcfg.Tnum),
		NumCFG_InstallShard: make(map[shardcfg.Tshid]shardcfg.Tnum),
		NumCFG_DeleteShard : make(map[shardcfg.Tshid]shardcfg.Tnum),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
