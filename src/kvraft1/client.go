package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
// 	args := rpc.GetArgs {
// 		Key: key,
// 	}
// 	reply := rpc.GetReply{}

// 	for i := 0 ; ; i ++ {
// 		time.Sleep (10 * time.Millisecond)
// 		i %= len(ck.servers)

// 		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
// 		if !ok {continue}
// 		if reply.Err == rpc.ErrWrongLeader {continue}

// 		break
// 	}

// 	if reply.Err == rpc.ErrNoKey {
// 		return "", 0, reply.Err
// 	}
// 	return reply.Value, reply.Version, reply.Err
// }
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
				break
			}
			
			z := atomic.LoadInt32(&done)
			if z != 0 {panic("error")}
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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
// 	t := make([]int,len(ck.servers))
// 	args := rpc.PutArgs {
// 		Key: key,
// 		Value: value,
// 		Version: version,
// 	}
// 	reply := rpc.PutReply{}

// 	i := 0 
// 	for ; ; i ++ {
// 		time.Sleep (10 * time.Millisecond);
// 		i %= len(ck.servers)

// 		t [i] ++
// 		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
// 		if !ok {continue}
// 		if reply.Err == rpc.ErrWrongLeader { t [i] = 0; continue}

// 		break
// 	}

// 	if t [i] > 1 && reply.Err == rpc.ErrVersion {
// 		return rpc.ErrMaybe
// 	}
// 	return reply.Err
// }

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
				break
			}

			z := atomic.LoadInt32(&leader)
			if z != -1 {panic("error")}
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
