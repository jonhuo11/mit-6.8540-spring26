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

type kvValue struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	// Your definitions here.

	kv  map[string]kvValue
	opQ chan kvOp
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}

	// Your code here.
	kv.kv = make(map[string]kvValue, 256)
	kv.opQ = make(chan kvOp, 256)
	go func() {
		for {
			op := <-kv.opQ
			switch op.t {
			case kvOpTypeGet:
				if v, ok := kv.kv[op.get.args.Key]; ok {
					op.get.replyChan <- rpc.GetReply{
						Value:   v.value,
						Version: v.version,
						Err:     rpc.OK,
					}
				} else {
					op.get.replyChan <- rpc.GetReply{
						Err:     rpc.ErrNoKey,
						Version: 0, // for lock Acquire()
					}
				}
			case kvOpTypePut:
				if v, ok := kv.kv[op.put.args.Key]; ok {
					if v.version != op.put.args.Version {
						op.put.replyChan <- rpc.PutReply{
							Err: rpc.ErrVersion,
						}
						continue
					}

					kv.kv[op.put.args.Key] = kvValue{
						value:   op.put.args.Value,
						version: v.version + 1,
					}
					op.put.replyChan <- rpc.PutReply{
						Err: rpc.OK,
					}
				} else if op.put.args.Version == 0 {
					kv.kv[op.put.args.Key] = kvValue{
						value:   op.put.args.Value,
						version: 1,
					}
					op.put.replyChan <- rpc.PutReply{
						Err: rpc.OK,
					}
				} else { // does not exist and user provided version != 0
					op.put.replyChan <- rpc.PutReply{
						Err: rpc.ErrNoKey,
					}
				}
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
	replyChan := make(chan rpc.PutReply, 0)
	kv.opQ <- kvOp{
		t: kvOpTypePut,
		put: kvOpPut{
			args:      *args,
			replyChan: replyChan,
		},
	}
	*reply = <-replyChan
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
