package lock

import (
	"time"

	kvtest "6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l string
	value string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l, value: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val , ver, _ := lk.ck.Get(lk.l)
		// if no one holds the lock, I will acquire it 
		if val == "" {
			err := lk.ck.Put(lk.l, lk.value, ver)
			if err == rpc.OK {
				return
			}
		} else if val == lk.value {
			return
		}
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	
}

func (lk *Lock) Release() {
	// Your code here
	t := 0 
	for {
		val , ver, _ := lk.ck.Get(lk.l)
		// if i hold the lock, I will release it
		if val == lk.value {
			t = 1 
			err := lk.ck.Put(lk.l, "", ver)
			if err == rpc.OK {
				return
			}
		} else if t == 1 {
			return
		}
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	
}
