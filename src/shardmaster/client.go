package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

import (
  log "github.com/Sirupsen/logrus"
)

func init() {
  // Only log the warning severity or above.
  // log.SetLevel(log.InfoLevel)
  log.SetLevel(log.DebugLevel)
  // log.SetLevel(log.WarnLevel)
  // log.SetLevel(log.ErrorLevel)
  // log.SetLevel(log.FatalLevel)
}

type Clerk struct {
	servers []*labrpc.ClientEnd

	uniqueClientID	int64
	commandNumber	int

	lastLeader		int
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

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(QueryReply)

	ck.commandNumber = ck.commandNumber + 1

	ok := ck.servers[ck.lastLeader].Call("ShardMaster.Query", &args, reply)
	if ok && reply.WrongLeader == false {
		return reply.Config
	}

	for {
		// try each known server.
		for srvNumber, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = srvNumber
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{Servers: servers, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(JoinReply)

	ck.commandNumber = ck.commandNumber + 1

	ok := ck.servers[ck.lastLeader].Call("ShardMaster.Join", &args, reply)
	if ok && reply.WrongLeader == false {
		return
	}

	for {
		// try each known server.
		for srvNumber, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = srvNumber
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{GIDs: gids, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(LeaveReply)

	ck.commandNumber = ck.commandNumber + 1

	ok := ck.servers[ck.lastLeader].Call("ShardMaster.Leave", &args, reply)
	if ok && reply.WrongLeader == false {
		return
	}

	for {
		// try each known server.
		for srvNumber, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = srvNumber
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{Shard: shard, GID: gid, CommandNumber: ck.commandNumber, ClientIDNumber: ck.uniqueClientID}
	reply := new(MoveReply)

	ck.commandNumber = ck.commandNumber + 1

	ok := ck.servers[ck.lastLeader].Call("ShardMaster.Move", &args, reply)
	if ok && reply.WrongLeader == false {
		return
	}

	for {
		// try each known server.
		for srvNumber, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = srvNumber
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
