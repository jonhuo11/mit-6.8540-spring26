package lock

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const emptyLockOwnerId string = ""

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
		fmt.Printf("lock client %v acquired lock %v\n", lk.id, lk.name)
	}()

	// spinlock until we acquire it
	// lockname: holder
	for {
		// get version + owner
		ownerId, version, err := lk.ck.Get(lk.name)
		unowned := err == rpc.ErrNoKey || ownerId == emptyLockOwnerId
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
				panic(fmt.Sprintf("lock %v: something fked up, lock key was deleted", lk.name))
			}
			if ownerId == lk.id {
				return
			}
			continue // spin, we didn't get it
		case rpc.ErrNoKey: // we fked up the version somehow
			panic(fmt.Sprintf("lock %v: we fked up the version num", lk.name))
		}
	}
}

func (lk *Lock) Release() {
	// Your code here

	defer func() {
		fmt.Printf("lock client %v released lock %v\n", lk.id, lk.name)
	}()

	// You must own the lock to release it, not owning = no-op
	ownerId, version, err := lk.ck.Get(lk.name)
	unowned := err == rpc.ErrNoKey || ownerId == emptyLockOwnerId
	if unowned || ownerId != lk.id { // we do not own it, no-op
		return
	}

	// put should always be OK as we own the lock, nobody else should be able to modify the version number
	err = lk.ck.Put(lk.name, emptyLockOwnerId, version)
	switch err {
	case rpc.ErrVersion:
		panic(fmt.Sprintf("lock %v was modified by non-owner or not through Lock somehow", lk.name))
	case rpc.ErrMaybe:
		// we either released it or failed to release it
		// fail to release -> someone else did? panic if so
		ownerId, _, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			panic(fmt.Sprintf("lock %v: something fked up the version num", lk.name))
		}
		if ownerId != emptyLockOwnerId {
			panic(fmt.Sprintf("lock %v was modified by non-owner or not through Lock somehow", lk.name))
		}
		// we released it successfully
	case rpc.ErrNoKey: // we fked up the version somehow in Put
		panic(fmt.Sprintf("lock %v was modified by non-owner or not through Lock somehow", lk.name))
	}
}
