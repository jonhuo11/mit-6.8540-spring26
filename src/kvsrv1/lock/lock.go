package lock

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id   string // unique identifier of this lock client
	name string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(32)
	lk.name = lockname
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	defer func() {
		fmt.Println("lock client %v acquired lock %v", lk.id, lk.name)
	}()

	// spinlock until we acquire it
	// lockname: holder
	for {
		// get version + owner
		ownerId, version, err := lk.ck.Get(lk.name)
		unowned := err == rpc.ErrNoKey || ownerId == ""
		if !unowned {
			if ownerId == lk.id {
				return
			} // we own it, no-op
			continue // spin
		}

		// try put (lockname, myId) as (lockname, ownerId)
		err = lk.ck.Put(lk.name, lk.id, version)
		switch err {
		case rpc.OK: // we got it, exit
			return
		case rpc.ErrVersion: // we did not get it, spin
			continue
		case rpc.ErrMaybe: // did we get it? need to check
			// The edge case here is:
			// - we send twice, first succeeds but server ACK dropped
			// - we recv a ErrVersion (client auto converts to ErrMaybe)
			// - did our initial one succeed but got an ErrVersion on our unneeded retry (and thus we own the lock)
			// 		or did someone else beat us and give ErrVersion? Can't tell
			// - thus we need to check ownership
			ownerId, _, err := lk.ck.Get(lk.name)
			if err == rpc.ErrNoKey {
				panic("lock: something fked up the version num")
			}
			if ownerId == lk.id {
				return
			}
			continue // spin, we didn't get it
		case rpc.ErrNoKey: // we fked up the version somehow
			panic("lock: we fked up the version num")
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// TODO:
}
