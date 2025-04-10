package shardgrp

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	var done int32
	atomic.StoreInt32(&done, 0)
	res := rpc.GetReply{}

	for i := 0 ; i < len(ck.servers) ; i ++ {
		go func(server int) {

			args := rpc.GetArgs {
				Key: key,
			}
			reply := rpc.GetReply{}
			
			for {
				z := atomic.LoadInt32(&done)
				if z != 0 {return}

				ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
				if !ok || reply.Err == rpc.ErrWrongLeader {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if reply.Err == rpc.ErrClosedApplyCh {
					return
				}
				break
			}
			
			z := atomic.LoadInt32(&done)
			if z != 0 {return; panic("error")}
			res = reply
			atomic.StoreInt32(&done, 1)

		}(i)
	}

	for {
		z := atomic.LoadInt32(&done)
		if z != 0 {break}
		time.Sleep(10 * time.Millisecond)
	}

	if res.Err == rpc.ErrNoKey {
		return "", 0, res.Err
	}
	return res.Value, res.Version, res.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	t := make([]int,len(ck.servers))
	var leader int32
	atomic.StoreInt32(&leader,-1)
	res := rpc.PutReply{}

	for i := 0 ; i < len(ck.servers) ; i ++ {
		go func(server int) {

			args := rpc.PutArgs {
				Key: key,
				Value: value,
				Version: version,
			}
			reply := rpc.PutReply{}

			for {
				z := atomic.LoadInt32(&leader)
				if z != -1 {return}
				
				
				ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)
				t[server]++
				if !ok || reply.Err == rpc.ErrWrongLeader {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if reply.Err == rpc.ErrClosedApplyCh {
					return
				}
				break
			}

			z := atomic.LoadInt32(&leader)
			if z != -1 {return; panic("error")}
			res = reply
			atomic.StoreInt32(&leader,int32(server))

		}(i)
	}

	for {
		z := atomic.LoadInt32(&leader)
		if z != -1 {break}
		time.Sleep(10 * time.Millisecond)
	}

	if t [leader] > 1 && res.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return res.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
