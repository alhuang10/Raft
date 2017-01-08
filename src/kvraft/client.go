package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "math"

import (
  log "github.com/Sirupsen/logrus"
)

func init() {
  // Only log the warning severity or above.
  // log.SetLevel(log.InfoLevel)
  // log.SetLevel(log.DebugLevel)
  // log.SetLevel(log.WarnLevel)
  log.SetLevel(log.ErrorLevel)
  // log.SetLevel(log.FatalLevel)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	uniqueClientID	int64
	commandNumber	int

	lastLeader		int // The sever we sent the last 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.uniqueClientID = nrand()
	ck.commandNumber = 1 // Incrementally increasing from 1 b/c starting map is 0 (null value when nothing entered yet)
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{Key: key, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(GetReply)

	ck.commandNumber = ck.commandNumber+1

	//Try the last leader first
	ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", &args, reply)
	if ok {
		if reply.WrongLeader == false {
			return reply.Value
		}
	}

	// If it doesn't work, then start trying each one 
	serverTryNumber := 0
	serverNumberToTry := 0
	numberOfServers := len(ck.servers)

	// Just go forever until we get a reply from a leader with a value then just return it
	for {
		reply = new(GetReply) // New reply for each *Actually only one active at a time so it's fine
		serverNumberToTry = int(math.Mod(float64(serverTryNumber), float64(numberOfServers)))

		ok := ck.servers[serverNumberToTry].Call("RaftKV.Get", &args, reply)

		if ok {
			if reply.WrongLeader == false {
				ck.lastLeader = serverNumberToTry
				return reply.Value
			}
		}
		serverTryNumber++
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	args := PutAppendArgs{Key: key, Value: value, Op: op, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(PutAppendReply)

	ck.commandNumber = ck.commandNumber+1

	//Try the last leader first
	ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", &args, reply)
	if ok {
		if reply.WrongLeader == false {
			return
		}
	}

	// If it doesn't work, then start trying each one 
	serverTryNumber := 0
	serverNumberToTry := 0
	numberOfServers := len(ck.servers)

	for {
		reply = new(PutAppendReply) // New reply for each *Actually only one active at a time so it's fine?
		serverNumberToTry = int(math.Mod(float64(serverTryNumber), float64(numberOfServers)))
		ok := ck.servers[serverNumberToTry].Call("RaftKV.PutAppend", &args, reply)

		if ok {
			if reply.WrongLeader == false {
				ck.lastLeader = serverNumberToTry
				return
			}
		}

		serverTryNumber++
	}	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
