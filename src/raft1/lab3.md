# Lab 3

## Part 3A: Leader election

The goal for Part 3A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost.

Run `make RUN="-run 3A" raft1` in the `src` directory to test your 3A code.

## Part 3B: log

Implement the leader and follower code to append new log entries, so that `make RUN="-run 3B" raft1` passes all tests.

## Part 3C: persistence

Complete the functions `persist()` and `readPersist()` in `raft.go` by adding code to save and restore persistent state. You will need to encode (or "serialize") the state as an array of bytes in order to pass it to the Persister. Use the `labgob` encoder; see the comments in `persist()` and `readPersist()`. `labgob` is like Go's `gob` encoder but prints error messages if you try to encode structures with lower-case field names. For now, pass `nil` as the second argument to `persister.Save()`. Insert calls to `persist()` at the points where your implementation changes persistent state. Once you've done this, and if the rest of your implementation is correct, you should pass all of the 3C tests.

## Part 3D: log compaction

Implement `Snapshot()` and the `InstallSnapshot` RPC, as well as the changes to Raft to support these (e.g, operation with a trimmed log). Your solution is complete when it passes the 3D tests (and all the previous Lab 3 tests).