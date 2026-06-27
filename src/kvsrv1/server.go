package kvsrv

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type kvOpType uint

const (
	kvOpTypeGet kvOpType = 0
	kvOpTypePut          = 1
)

type kvOpGet struct {
	args      rpc.GetArgs
	replyChan chan rpc.GetReply
}

type kvOpPut struct {
	args      rpc.PutArgs
	replyChan chan rpc.PutReply
}

type kvOp struct {
	t   kvOpType
	get kvOpGet
	put kvOpPut
}

type KVServer struct {
	// Your definitions here.

	opQ chan kvOp
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}

	// Your code here.
	kv.opQ = make(chan kvOp, 128)
	go func() {
		for {
			op := <-kv.opQ
			switch op.t {
			case kvOpTypeGet:
			case kvOpTypePut:
			}
		}
	}()
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	/*
		replyChan := make(chan ...)
		kv.opQ <- op{opArgs: {...}, replyChan: replyChan}
		result := <-replyChan // block on this w timeout
		write reply to server
	*/
	replyChan := make(chan rpc.GetReply, 0)
	kv.opQ <- kvOp{
		t: kvOpTypeGet,
		get: kvOpGet{
			args:      *args,
			replyChan: replyChan,
		},
	}
	*reply = <-replyChan
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
