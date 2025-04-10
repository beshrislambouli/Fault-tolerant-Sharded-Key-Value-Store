package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
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
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.IKVClerk.Put("cfg",cfg.String(),0);
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new_cfg *shardcfg.ShardConfig) {
	// Your code here.

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

		//"freeze" the shard at the source shardgrp, causing that shardgrp to reject Put's
		State, _ := old_ck.FreezeShard(shardcfg.Tshid(sh), new_cfg.Num)

		//copy (install) the shard to the destination shardgrp
		new_ck.InstallShard(shardcfg.Tshid(sh), State, new_cfg.Num)

		//delete the frozen shard
		old_ck.DeleteShard(shardcfg.Tshid(sh), new_cfg.Num)
	}

	sck.IKVClerk.Put("cfg",new_cfg.String(),V)
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, _ := sck.IKVClerk.Get("cfg");
	return shardcfg.FromString(cfg);
}

