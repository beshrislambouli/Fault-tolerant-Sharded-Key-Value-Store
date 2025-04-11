package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	// "log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.

	// scfg := shardcfg.MakeShardConfig()
	// scfg.JoinBalance(map[tester.Tgid][]string{shardcfg.Gid1: []string{"xxx"}})
	// sck.InitConfig(scfg)

	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	// log.Printf("Get Client: on key %v",key)
	for {
		cfg := ck.sck.Query()
		_, servers, _ := cfg.GidServers(shardcfg.Key2Shard(key));
		// log.Printf("Get Client: current cfg: %v, server to ask: %v",cfg,servers)

		ck_grp := shardgrp.MakeClerk(ck.clnt, servers);

		res, v , err := ck_grp.Get(key);
		// log.Printf("Get Client: Got %v %v %v", res, v, err)
		if err == rpc.ErrWrongGroup {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return res, v , err
	}
	
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	// log.Printf("Put Client: on key %v",key)
	for {
		cfg := ck.sck.Query()
		_, servers, _ := cfg.GidServers(shardcfg.Key2Shard(key));
		// log.Printf("Put Client: current cfg: %v, server to ask: %v",cfg,servers)

		ck_grp := shardgrp.MakeClerk(ck.clnt, servers);

		err := ck_grp.Put(key,value,version);
		// log.Printf("Put Client: Got %v", err)

		if err == rpc.ErrWrongGroup {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return err
	}
	
}
