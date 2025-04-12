package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	// "log"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	// "6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"

	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	next_cfg_string, _, err := sck.IKVClerk.Get("next_cfg")
	if err != rpc.OK {return}
	next_cfg := shardcfg.FromString(next_cfg_string)

	curr_cfg_string, _, err := sck.IKVClerk.Get("cfg")
	if err != rpc.OK {return}
	curr_cfg := shardcfg.FromString(curr_cfg_string)

	// log.Printf("InitController %v %v",curr_cfg.Num,next_cfg.Num)
	
	if next_cfg.Num > curr_cfg.Num {
		sck.ChangeConfigTo(next_cfg)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here

	// log.Printf("InitConfig %v",cfg.Num)

	sck.IKVClerk.Put("cfg",cfg.String(),0);
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new_cfg *shardcfg.ShardConfig) {
	// Your code here.
	// log.Printf("ChangeConfigTo %v",new_cfg.Num)
	// update the next_cfg
	_, V, _ := sck.IKVClerk.Get("next_cfg")
	sck.IKVClerk.Put("next_cfg",new_cfg.String(),V)

	// get the old_cfg
	cfg, V, _ := sck.IKVClerk.Get("cfg");
	old_cfg := shardcfg.FromString(cfg);
	
	// kvsrv.DPrintf("%v",old_cfg)
	// kvsrv.DPrintf("%v",new_cfg)

	for sh := 0 ; sh < len(old_cfg.Shards) ; sh ++ {
		old_g := old_cfg.Shards[sh]
		new_g := new_cfg.Shards[sh]
		if old_g == new_g {continue}

		old_servers := old_cfg.Groups[old_g];
		new_servers := new_cfg.Groups[new_g];

		old_ck := shardgrp.MakeClerk(sck.clnt, old_servers);
		new_ck := shardgrp.MakeClerk(sck.clnt, new_servers); 

		// log.Printf("Controler: Shard %v to move from group %v: %v to group %v: %v",sh, old_g, old_servers, new_g, new_servers)

		//"freeze" the shard at the source shardgrp, causing that shardgrp to reject Put's
		// log.Printf("Sent FreezeShard, %v", new_cfg.Num);
		State, _ := old_ck.FreezeShard(shardcfg.Tshid(sh), new_cfg.Num)
		// log.Printf("Got FreezeShard, %v", err1);

		//copy (install) the shard to the destination shardgrp
		// log.Printf("Sent InstallShard, %v", new_cfg.Num);
		new_ck.InstallShard(shardcfg.Tshid(sh), State, new_cfg.Num)
		// log.Printf("Got InstallShard, %v", err2);

		//delete the frozen shard
		// log.Printf("Sent DeleteShard, %v", new_cfg.Num);
		old_ck.DeleteShard(shardcfg.Tshid(sh), new_cfg.Num)
		// log.Printf("Got DeleteShard, %v", err3);
	}
	// log.Printf("Install new cfg %v", new_cfg);
	sck.IKVClerk.Put("cfg",new_cfg.String(),V)
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, _ := sck.IKVClerk.Get("cfg");
	return shardcfg.FromString(cfg);
}

