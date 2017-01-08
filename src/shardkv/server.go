package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"
import "bytes"

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

// Operation Enum
const (
	GET = 0
	PUT = 1
	APPEND = 2
	CONFIG = 3
	SHARD_DATA = 4
	CONFIG_DONE = 5
)


const Debug = 0
const SHUTDOWN_CHANNEL_TIMEOUT = 200
const POLL_FOR_CONFIG = 100

func assert(is_true bool, message string) {
    if !is_true {
        panic(message)
    }
}

type Op struct {
	UniqueClientID	int64 // from nrand()
	ClientCommandNumber int
	OperationType		int // 'get' 'append' 'put'
	Key			string
	Value 		string // '' if it's a get
	Shard 		int

	// ConfigHasChanged	bool // Check this upon converting to Op stuct in the apply channel loop
	StartingConfig 		int
	TransitionConfig 	int
	ShardsGained 	[]int
	ShardsLost 		[]int
	NewShardAssignment 	[]int
	OwnedShards []int

	ConfigDuringRPCHandling 	int

	Groups map[int][]string
	// For sending shard info over log
	// Use *Shard* and *ConfigNum* from above 
	KvDict 	map[string]string
	ClientCommandsSeen 	map[int64]CommandNumberAndValue
}

// Capitalized because we need to encode
type CommandNumberAndValue struct {

	CommandNumber 	int
	Value 	string
}

type ClientIDTermChannel struct {
	UniqueClientID int64
	Term 	int
	CommandShutdownChannel chan ShutdownValue
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// For snapshots or something
	HighestIndexSeen	int
	HighestTermSeen		int 

	// Lab 4 Persisted stuff
	CurrentConfigNum	int
	PendingConfigNum 	int
	OwnedShards		[]int // Shards it owns, if a key it gets is corresponds to one of these shards fulfill the request
	ShardToKvDict	map[int]map[string]string // The KV dictionary corresponding to each shard
	ShardToLargestSeenClientCommandMap 	map[int]map[int64]CommandNumberAndValue	// The map that keeps track of highest seen commands from clients and the value corresponding to that command
	PersistedShardSendingInfo 	[]ShardInfo // maps from shard to all the info you need to send and to send it
	ShardsPending	[]int //shards taht we are waiting for in order to continue
	CurrentlyTransitioningConfig bool


	kvraftShutdownChannel	chan int

	shardToClientCommandShutdownChannelMap	map[int]map[int][]ClientIDTermChannel

	// Lets us query the clerk
	mck 	*shardmaster.Clerk

	// When we get notified that we've lost a shard, close the channel that the shard int maps to to stop all waiting requests
	// Remember to remake the channel when you receive a shard
	shardsLostChannel	map[int]chan int

	configPollTimer		*time.Timer
	checkPendingShardsTimer 	*time.Timer

	shardToShutdownChannel 	map[int]chan int
}


type ShardInfo struct {
	Shard 	int
	TransitionConfigNum 	int
	ServersToSendTo 	[]string
	KvDict 	map[string]string
	LargestSeenClientCommandMap 	map[int64]CommandNumberAndValue
}

type ShutdownValue struct {

	ClientReturnValue string
	Succeeded	bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	log.WithFields(log.Fields{"Shard": args.Shard,
		"Server": kv.me, 
		"Key": args.Key,
		"GID": kv.gid,
		"Owned Shards": kv.OwnedShards,
		"Owns The Shard?": kv.ownsShard(args.Shard),}).Info("New Get")

	kv.mu.Lock()

	if kv.ownsShard(args.Shard) {

		commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: GET, Key: args.Key, Value: "", Shard: args.Shard, ConfigDuringRPCHandling: kv.CurrentConfigNum}
		index, term, isLeader := kv.rf.Start(commandToLog)

		if isLeader {

			shutdownChannelForCommand := make(chan ShutdownValue)
			commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
			

			_, shardInMap := kv.shardToClientCommandShutdownChannelMap[args.Shard]

			if shardInMap == false {
				kv.shardToClientCommandShutdownChannelMap[args.Shard] = make(map[int][]ClientIDTermChannel)
			}

			currentSlice, inMap := kv.shardToClientCommandShutdownChannelMap[args.Shard][index]

			if inMap {
				kv.shardToClientCommandShutdownChannelMap[args.Shard][index] = append(currentSlice, commandShutdown)
			} else {
				newSlice := []ClientIDTermChannel{commandShutdown}
				kv.shardToClientCommandShutdownChannelMap[args.Shard][index] = newSlice
			}

			kv.mu.Unlock()

			timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

			select{
			case <- timeout.C:

				reply.WrongLeader = true
				reply.Err = OK
				kv.mu.Lock()
				kv.deleteCommandChannel(index, args.ClientIDNumber, args.Shard)
				kv.mu.Unlock()
				return
			case <- kv.shardsLostChannel[args.Shard]: // if we lose a shard we close this channel, ending any outstanding requests

				reply.Err = ErrWrongGroup
				// Need to delete from here or no?
				kv.mu.Lock()
				kv.deleteCommandChannel(index, args.ClientIDNumber, args.Shard)
				kv.shardsLostChannel[args.Shard] = make(chan int)
				kv.mu.Unlock()
				return

			case shutdownValue := <-shutdownChannelForCommand:

				if shutdownValue.Succeeded {

					reply.WrongLeader = false
					reply.Err = OK
					reply.Value = shutdownValue.ClientReturnValue // should be non null string
					return
				} else {
					reply.WrongLeader = true
					reply.Err = OK
					return
				}				
			}
		} else {

			kv.mu.Unlock()
			reply.WrongLeader = true
			reply.Err = OK
			return
		}

	} else {
		log.Error("Wrong Group, if we get a shit ton of these we messsed up and we are supposed to have something we don't")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
	} 
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	log.WithFields(log.Fields{"Shard": args.Shard,
		"Server": kv.me, 
		"Key": args.Key,
		"GID": kv.gid,
		"Value": args.Value,
		"Operation": args.Op,
		"Owned Shards": kv.OwnedShards,
		"Owns The Shard?": kv.ownsShard(args.Shard),}).Info("New PutAppend")

	kv.mu.Lock() 

	if kv.ownsShard(args.Shard) {

		opType := PUT
		if args.Op == "Append" {
			opType = APPEND
		}

		commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: opType, Key: args.Key, Value: args.Value, Shard: args.Shard, ConfigDuringRPCHandling: kv.CurrentConfigNum}
		index, term, isLeader := kv.rf.Start(commandToLog)

		if isLeader {

			shutdownChannelForCommand := make(chan ShutdownValue)
			commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
			
			_, shardInMap := kv.shardToClientCommandShutdownChannelMap[args.Shard]

			if shardInMap == false {
				kv.shardToClientCommandShutdownChannelMap[args.Shard] = make(map[int][]ClientIDTermChannel)
			}

			currentSlice, inMap := kv.shardToClientCommandShutdownChannelMap[args.Shard][index]

			if inMap {
				kv.shardToClientCommandShutdownChannelMap[args.Shard][index] = append(currentSlice, commandShutdown)
			} else {
				newSlice := []ClientIDTermChannel{commandShutdown}
				kv.shardToClientCommandShutdownChannelMap[args.Shard][index] = newSlice
			}

			kv.mu.Unlock()

			timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

			select{
			case <- timeout.C:

				reply.WrongLeader = true
				reply.Err = OK
				kv.mu.Lock()
				kv.deleteCommandChannel(index, args.ClientIDNumber, args.Shard)
				kv.mu.Unlock()
				return
			case <- kv.shardsLostChannel[args.Shard]: // if we lose a shard we close this channel, ending any outstanding requests

				reply.Err = ErrWrongGroup
				// Need to delete from here or no?
				kv.mu.Lock()
				kv.deleteCommandChannel(index, args.ClientIDNumber, args.Shard)
				kv.shardsLostChannel[args.Shard] = make(chan int)
				kv.mu.Unlock()
				return

			case shutdownValue := <-shutdownChannelForCommand:

				if shutdownValue.Succeeded {

					reply.WrongLeader = false
					reply.Err = OK
					return
				} else {
					reply.WrongLeader = true
					reply.Err = OK
					return
				}				
			}
		} else {

			kv.mu.Unlock()
			reply.WrongLeader = true
			reply.Err = OK
			return
		}

	} else {
		log.Error("Wrong Group, if we get a shit ton of these we messsed up and we are supposed to have something we don't")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
	} 
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {

	_,isLeader := kv.rf.GetState()

	log.WithFields(log.Fields{
		"Leader?": isLeader,
		"GID": kv.gid, 
		"Server": kv.me,
		"ItsCurrentConfig": kv.CurrentConfigNum,
		"CurrentlyTransitioning": kv.CurrentlyTransitioningConfig,}).Warn("Killing a server")

	kv.rf.Kill()
	close(kv.kvraftShutdownChannel)
}

func (kv *ShardKV) shardInSlice(shard int, shardSlice []int) bool {

	for _, shardItem := range shardSlice {

		if shard == shardItem {
			return true
		}
	}
	return false
}

func (kv *ShardKV) deleteFromSlice(shardToDelete int, shardSlice []int) []int {

	deleteIndex := -1
	newSlice := make([]int, len(shardSlice))
	copy(newSlice, shardSlice)

	for index, shardValue := range shardSlice {

		if shardValue == shardToDelete {
			deleteIndex = index
			break
		}
	}

	if deleteIndex != -1 {

		newSlice = append(shardSlice[:deleteIndex], shardSlice[deleteIndex+1:]...)
	}

	return newSlice
}

func (kv *ShardKV) ownsShard(shardValue int) bool {

	for _, shard := range kv.OwnedShards {

		if shard == shardValue {
			return true
		}
	}
	return false
}

func (kv *ShardKV) persist(persister *raft.Persister) {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.HighestIndexSeen) 
	e.Encode(kv.HighestTermSeen)
	e.Encode(kv.CurrentConfigNum)
	e.Encode(kv.PendingConfigNum)
	e.Encode(kv.OwnedShards)
	e.Encode(kv.ShardToKvDict)
	e.Encode(kv.ShardToLargestSeenClientCommandMap)
	e.Encode(kv.PersistedShardSendingInfo)
	e.Encode(kv.ShardsPending)
	e.Encode(kv.CurrentlyTransitioningConfig)
	data := w.Bytes()
	persister.SaveSnapshot(data)
	
}

func (kv *ShardKV) readPersist(data []byte) {

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.HighestIndexSeen) 
	d.Decode(&kv.HighestTermSeen)
	d.Decode(&kv.CurrentConfigNum)
	d.Decode(&kv.PendingConfigNum)
	d.Decode(&kv.OwnedShards)
	d.Decode(&kv.ShardToKvDict)
	d.Decode(&kv.ShardToLargestSeenClientCommandMap)
	d.Decode(&kv.PersistedShardSendingInfo)
	d.Decode(&kv.ShardsPending)
	d.Decode(&kv.CurrentlyTransitioningConfig)
}	


func (kv *ShardKV) deleteCommandChannel(index int, clientID int64, shard int) {

	// Iterate through the slice mapped to by the provided index until you get to the ClientIDTermChannel struct that has a matching clientID
	// Keep track of which index of the slice you're on and remove from
	indexToDelete := -1

	for index, element := range kv.shardToClientCommandShutdownChannelMap[shard][index] {
		if element.UniqueClientID == clientID {
			indexToDelete = index
		}
	}

	// Two sources of deletion, one from timeout and one if another command arrived at that same index
	if indexToDelete != -1 {
		currentSlice := kv.shardToClientCommandShutdownChannelMap[shard][index]
		kv.shardToClientCommandShutdownChannelMap[shard][index] = append(currentSlice[:indexToDelete], currentSlice[indexToDelete+1:]...)
	}
}

// Get the shards that we lost in new config
func (kv *ShardKV) getLostShards(newConfig shardmaster.Config) []int {

	lostShards := make([]int, 0)
	newShardAssignment := newConfig.Shards

	for _, shard := range kv.OwnedShards {
		if newShardAssignment[shard] != kv.gid { // Shard we used to have now has new GID
			lostShards = append(lostShards, shard)
		}
	}
	return lostShards
}

// Get the shards that we gained in new config
func (kv *ShardKV) getGainedShards(newConfig shardmaster.Config) []int {

	gainedShards := make([]int, 0)
	newShardAssignment := newConfig.Shards

	for shard, gid := range newShardAssignment {

		if kv.ownsShard(shard) == false {
			if gid == kv.gid { // before it didn't own but now new shard configuration says it does
				gainedShards = append(gainedShards, shard)
			}
		}
	}
	return gainedShards
}


// We have a lock while calling this in apply channel
func (kv *ShardKV) sendShardInfo(shardInfo ShardInfo) { 
	// Send shard info which we currently own to new GID that owns it
	// Send the kv dictionary and largest seen cilent command map
		// Remember to copy before putting it in the arguments
	// Just go until reply is true similar to the stuff in client.go

	log.WithFields(log.Fields{
		"GID that is sending": kv.gid, 
		"Shard": shardInfo.Shard,}).Info("Sending a lost shard")

	args := AcceptShardArgs{Shard: shardInfo.Shard, TransitionConfigNum: shardInfo.TransitionConfigNum, KvDict: shardInfo.KvDict, ClientCommandsSeen: shardInfo.LargestSeenClientCommandMap}

	serversToSendTo := shardInfo.ServersToSendTo

	for {
		select {
		case <- kv.kvraftShutdownChannel:	
			return
		default:
			for si := 0; si < len(serversToSendTo); si++ {
				var reply AcceptShardReply
				srv := kv.make_end(serversToSendTo[si])
				ok := srv.Call("ShardKV.AcceptShard", &args, &reply)
				if ok {
					if reply.WrongLeader == false && reply.Err == OK {

						// Delete it from the list 
						kv.mu.Lock()

						deleteIndex := -1
						for index, shardInfo := range kv.PersistedShardSendingInfo {

							if shardInfo.Shard == args.Shard && shardInfo.TransitionConfigNum == args.TransitionConfigNum {
								deleteIndex = index
								break
							}
						}

						if deleteIndex != -1 {
							kv.PersistedShardSendingInfo = append(kv.PersistedShardSendingInfo[:deleteIndex], kv.PersistedShardSendingInfo[deleteIndex+1:]...)
						}

						kv.mu.Unlock()

						return
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) AcceptShard(args *AcceptShardArgs, reply *AcceptShardReply) {

	// Get the kvdict and clientinfo from AcceptShardArgs
	// Start shutdown channels that correspond to shards
	// When we see the value arriving through the apply channel, then we return and reply

	kv.mu.Lock()

	// If this isn't the config we're trying to go to then return
	if kv.CurrentlyTransitioningConfig == false || kv.PendingConfigNum != args.TransitionConfigNum {

		kv.mu.Unlock()
		return
	} 

	commandToLog := Op{OperationType: SHARD_DATA, Shard: args.Shard, StartingConfig: kv.CurrentConfigNum, TransitionConfig: args.TransitionConfigNum, KvDict: args.KvDict, ClientCommandsSeen: args.ClientCommandsSeen}
	_, _, isLeader := kv.rf.Start(commandToLog)

 	// Add a check for if we're going to the right configuration

	if isLeader {

		log.WithFields(log.Fields{
			"GID that is accepting shard": kv.gid, 
			"Shard": args.Shard,}).Info("Adding shutdown channel for shard")

		shutdownChannelForShard := make(chan int)
		kv.shardToShutdownChannel[args.Shard] = shutdownChannelForShard

		kv.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)
		select {
		case <- timeout.C:
			reply.WrongLeader = true
			return
		case <- shutdownChannelForShard:

			reply.Err = OK
			reply.WrongLeader = false
			return
		}
	} else {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(CommandNumberAndValue{}) // Value of a dictionary we send over rpc

	kv := new(ShardKV)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.kvraftShutdownChannel = make(chan int)

	kv.CurrentConfigNum = 0
	kv.PendingConfigNum = 0
	kv.OwnedShards = make([]int, 0)


	kv.ShardToKvDict = make(map[int]map[string]string)
	kv.ShardToLargestSeenClientCommandMap = make(map[int]map[int64]CommandNumberAndValue)
	kv.shardToClientCommandShutdownChannelMap = make(map[int]map[int][]ClientIDTermChannel)

	kv.shardsLostChannel = make(map[int]chan int)

	for i:=0; i<10; i++ {
		kv.shardsLostChannel[i] = make(chan int)
	}

	kv.configPollTimer = time.NewTimer(time.Duration(POLL_FOR_CONFIG) * time.Millisecond)
	kv.checkPendingShardsTimer = time.NewTimer(time.Duration(20) * time.Millisecond)

	kv.ShardsPending = make([]int, 0)
	
	kv.PersistedShardSendingInfo = make([]ShardInfo, 0)

	kv.shardToShutdownChannel = make(map[int]chan int)


	kv.CurrentlyTransitioningConfig = false

	log.WithFields(log.Fields{
		"GID": kv.gid,
		"Server": kv.me,}).Warn("Starting up again")	// kv.readPersist(persister.ReadSnapshot())	


	if kv.maxraftstate != -1 {
		kv.readPersist(persister.ReadSnapshot())

		if len(kv.PersistedShardSendingInfo) != 0 {

			log.Error("We had stuff that we didn't finish sending but we died, restarting the attempted sends so stuff can play back")
			for _, shardInfo := range kv.PersistedShardSendingInfo {

				go kv.sendShardInfo(shardInfo)
			}
		}
	} 
	
	// Only start this raft size checking go routine if necesssary
	if maxraftstate != -1 { 
		go func() {
			for {
				select{
				case <-kv.kvraftShutdownChannel:
					return
				default:
					kv.mu.Lock()
					IndexToSendInSnapshot := kv.HighestIndexSeen

					if persister.RaftStateSize() > maxraftstate * 3/4 {

						// Takes the snapshot and tells raft about it
						kv.persist(persister)

						// Let the raft know the index it can delete up to b/c encapsulated by snapshot
						kv.rf.TrimLogsViaSnapshot(IndexToSendInSnapshot-1)
						kv.mu.Unlock()
					} else {
						kv.mu.Unlock()
					}

					time.Sleep(time.Duration(200) * time.Millisecond)
				}
			}
		}()
	}

	go func() {
		for {
			select{
			case <- kv.kvraftShutdownChannel:
				return
			case <-kv.configPollTimer.C:

				kv.mu.Lock()

				latestConfig := kv.mck.Query(-1)

				if latestConfig.Num > kv.CurrentConfigNum { 

					_,isLeader := kv.rf.GetState()

					// Checking beforehand for leader
					if isLeader {

						if kv.CurrentlyTransitioningConfig == false && latestConfig.Num > kv.PendingConfigNum { // not trying to go to a larger config
						// if kv.CurrentConfigNum == kv.PendingConfigNum { // not trying to go to a larger config

							log.WithFields(log.Fields{
								"kv.CurrentConfigNum": kv.CurrentConfigNum,
								"kv.PendingConfigNum": kv.PendingConfigNum+1,
								"Stuff in kvdict": len(kv.ShardToKvDict),
								"Shards Pending": kv.ShardsPending,
								"GID": kv.gid,
								"Server": kv.me,}).Warn("Trying to increase config")

							configToTransitionTo := kv.mck.Query(kv.CurrentConfigNum + 1)

							shardsLost := kv.getLostShards(configToTransitionTo) // slice
							shardsGained := kv.getGainedShards(configToTransitionTo)

							// Copying into slice b/c I can't figure how to specify array variable type...
							newShardAssignment := make([]int, 0)
							for _,gid := range configToTransitionTo.Shards {
								newShardAssignment = append(newShardAssignment, gid)
							}

							duplicateGroups := make(map[int][]string)
							for gid, serverNames := range configToTransitionTo.Groups {
								serverNamesCopy := make([]string, 0)
								for _,serverName := range serverNames {
									serverNamesCopy = append(serverNamesCopy, serverName)
								}
								duplicateGroups[gid] = serverNamesCopy
							}

							configCommandToLog := Op{OperationType: CONFIG, 
								StartingConfig: kv.CurrentConfigNum,
								TransitionConfig: configToTransitionTo.Num, 
								ShardsLost: shardsLost, 
								ShardsGained: shardsGained, 
								NewShardAssignment: newShardAssignment, 
								Groups: duplicateGroups}

							// If leader it will add, if not it just will return false and not add
							// If this fails it's actually fine because we only start transition when we get it from channel
							_,_,isLeader := kv.rf.Start(configCommandToLog)

							if isLeader {
								log.WithFields(log.Fields{
									"Server": kv.me, 
									"NewShardConfig": newShardAssignment,
									"kv.CurrentConfigNum": kv.CurrentConfigNum,
									"kv.PendingConfigNum": configToTransitionTo.Num,
									"GID": kv.gid,}).Warn("Submitting new config to log as leader")
							}

						}
					}
				}
				kv.mu.Unlock()

				kv.configPollTimer.Reset(time.Duration(POLL_FOR_CONFIG) * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			select {
			case <- kv.kvraftShutdownChannel:	
				return
			case applyChannelMessage := <- kv.applyCh:

				kv.mu.Lock() 

				assert((kv.CurrentConfigNum == kv.PendingConfigNum || kv.CurrentConfigNum == kv.PendingConfigNum-1), "shit is fucked up")

				if applyChannelMessage.UseSnapshot{

					//Only apply this if it is a higher index one
					if applyChannelMessage.Term >= kv.HighestTermSeen && applyChannelMessage.Index-1 >= kv.HighestIndexSeen {
						data := applyChannelMessage.Snapshot

						persister.SaveSnapshot(data)
						kv.readPersist(data)
					}

					kv.mu.Unlock()
					continue
				}

				currentCommand := applyChannelMessage.Command.(Op)

				log.WithFields(log.Fields{
					"OperationType": currentCommand.OperationType,
					"GID": kv.gid,
					"Server": kv.me,}).Info("Operation Type")

				if currentCommand.OperationType == SHARD_DATA {

					// Only accept shard data if you're transitiong or else you could update with stale data
					if currentCommand.TransitionConfig == kv.PendingConfigNum && kv.CurrentlyTransitioningConfig == true && kv.shardInSlice(currentCommand.Shard, kv.ShardsPending) {

						shard := currentCommand.Shard

						// if kv.shardInSlice(shard, kv.ShardsPending) {

						duplicateKvDict := make(map[string]string)
						for key, value := range currentCommand.KvDict {

							duplicateKvDict[key] = value
						}

						duplicateClientInfo := make(map[int64]CommandNumberAndValue)
						for key, cnv := range currentCommand.ClientCommandsSeen {

							cnvCopy := CommandNumberAndValue{CommandNumber: cnv.CommandNumber, Value: cnv.Value}
							duplicateClientInfo[key] = cnvCopy
						}

						// What keeps track of us advancing config
						kv.ShardsPending = kv.deleteFromSlice(shard, kv.ShardsPending)

						kv.ShardToKvDict[shard] = duplicateKvDict
						kv.ShardToLargestSeenClientCommandMap[shard] = duplicateClientInfo

						log.WithFields(log.Fields{
							"Transition Config": currentCommand.TransitionConfig,
							"Current Config": kv.CurrentConfigNum,
							"Shard": currentCommand.Shard, 
							"ShardsPending": kv.ShardsPending, 
							"Server": kv.me,
							"GID": kv.gid,}).Info("Shard data received, ShardsPending shows what's left")

						if len(kv.ShardsPending) == 0 {
							kv.CurrentlyTransitioningConfig = false
							kv.CurrentConfigNum = kv.PendingConfigNum

							log.WithFields(log.Fields{
								"New Config": kv.CurrentConfigNum,
								"GID": kv.gid,
								"Shard": shard, 
								"OwnedShards": kv.OwnedShards,}).Warn("Stopped transitioning, settled on new config")
						}

						kv.mu.Unlock()

						select{
						case kv.shardToShutdownChannel[shard]<-0: //indicates success
							log.Info("Sent to shutdown channel")
						default:
						}	
						continue
					}

					kv.mu.Unlock()
					continue
				}


				if currentCommand.OperationType == CONFIG {

					log.WithFields(log.Fields{
						"Server": kv.me,
						"Shards Gained": currentCommand.ShardsGained,
						"Shards Lost": currentCommand.ShardsLost,
						"Current Config": kv.CurrentConfigNum, 
						"Command Starting Config": currentCommand.StartingConfig,
						"Command Transition Config": currentCommand.TransitionConfig,
						"Pending Shards": kv.ShardsPending,
						"GID": kv.gid,}).Warn("Got a config from apply channel")

					_ , isLeader := kv.rf.GetState()

					// If the starting config number of this transition matches what we have
					if currentCommand.StartingConfig == kv.CurrentConfigNum {

						// We will have hella duplicates but it's idempotent so it's fine

						// This field officially starts the transformation because we then stop accepting apply channel stuff while this is true
						kv.CurrentlyTransitioningConfig = true
						kv.PendingConfigNum = kv.CurrentConfigNum + 1
						
						// Update for everyone:
							// ownedShards
							// pendingShards
						// Only Leader:
							// Start goroutines that send shit
						for _, shard := range currentCommand.ShardsGained {

							if kv.shardInSlice(shard, kv.OwnedShards) == false {
								kv.OwnedShards = append(kv.OwnedShards, shard)
							}

							// If we are not at config this one had a previous owner so add to pending
							if kv.CurrentConfigNum != 0 {				

								if kv.shardInSlice(shard, kv.ShardsPending) == false {
									kv.ShardsPending = append(kv.ShardsPending, shard)
								}
							}
						}

						// Take care of sending each of the lost shards
						for _,shard := range currentCommand.ShardsLost {

							// If we currently own the shard we lost, then delete it
							// If we are the leader start sending it
							if kv.shardInSlice(shard, kv.OwnedShards) {

								kv.OwnedShards = kv.deleteFromSlice(shard, kv.OwnedShards)

							// If we're the leader call a goroutine that will send the shit after we delete it
								if isLeader {

									
									// kv.copyShardInfoToPersisted(shard, currentCommand.Groups[newGID], kv.PendingConfigNum)

									// Create a shard info and add it to the persisted list of things that we need to send to other servers and start the goroutine
									duplicateKvDict := make(map[string]string)
									for key, value := range kv.ShardToKvDict[shard] {

										duplicateKvDict[key] = value
									}

									duplicateClientInfo := make(map[int64]CommandNumberAndValue)
									for key, cnv := range kv.ShardToLargestSeenClientCommandMap[shard] {

										cnvCopy := CommandNumberAndValue{CommandNumber: cnv.CommandNumber, Value: cnv.Value}
										duplicateClientInfo[key] = cnvCopy
									}

									newGID := currentCommand.NewShardAssignment[shard]
									serversToSendTo := currentCommand.Groups[newGID]
									newShardInfo := ShardInfo{Shard: shard, TransitionConfigNum: kv.PendingConfigNum, ServersToSendTo: serversToSendTo, KvDict: duplicateKvDict, LargestSeenClientCommandMap: duplicateClientInfo}

									alreadyInListToSend := false
									for _, shardInfo := range kv.PersistedShardSendingInfo {

										if newShardInfo.Shard == shardInfo.Shard && newShardInfo.TransitionConfigNum == shardInfo.TransitionConfigNum {

											alreadyInListToSend = true
										}
									}

									if alreadyInListToSend == false {

										kv.PersistedShardSendingInfo = append(kv.PersistedShardSendingInfo, newShardInfo)
										go kv.sendShardInfo(newShardInfo)
									}
									// close(kv.shardsLostChannel[shard])
									
								}
							}

							// Challenge 1 stuff
							delete(kv.ShardToKvDict, shard)		
							delete(kv.ShardToLargestSeenClientCommandMap, shard)				
						}

						log.WithFields(log.Fields{
							"Server": kv.me,
							"Shards Gained": currentCommand.ShardsGained,
							"Shards Lost": currentCommand.ShardsLost,
							"Current Config": kv.CurrentConfigNum, 
							"Command Starting Config": currentCommand.StartingConfig,
							"Command Transition Config": currentCommand.TransitionConfig,
							"Pending Shards": kv.ShardsPending,
							"GID": kv.gid,}).Warn("Entering Transition")

						// If there are no shards we need to receive just advance it
						if len(kv.ShardsPending) == 0 {

							kv.CurrentlyTransitioningConfig = false
							kv.CurrentConfigNum = kv.PendingConfigNum

							log.WithFields(log.Fields{
								"New Config": kv.CurrentConfigNum,
								"GID": kv.gid,
								"Server": kv.me, 
								"OwnedShards": kv.OwnedShards,}).Warn("Didn't have any shards to receive so just advances")
						}

						// Encompasses all the persisted values in the loop above because 
					}

					kv.mu.Unlock()
					continue
				}
				
				if kv.CurrentlyTransitioningConfig == true {

					if kv.CurrentConfigNum != currentCommand.ConfigDuringRPCHandling ||  kv.shardInSlice(currentCommand.Shard, kv.ShardsPending) == true {
						kv.mu.Unlock()
						continue
					}
				} else {
					// If we not transitioning just do a base check on the configs

					// If the configs don't match up or we're currently trying to get this slice
					if kv.CurrentConfigNum != currentCommand.ConfigDuringRPCHandling {

						kv.mu.Unlock()
						continue
					}
				}

				clientID := currentCommand.UniqueClientID
				commandIndex := applyChannelMessage.Index
				currentTerm := kv.rf.CurrentTerm
				shardResponsible := currentCommand.Shard

				_, shardInKVDict := kv.ShardToKvDict[shardResponsible]

				if shardInKVDict == false {
					kv.ShardToKvDict[shardResponsible] = make(map[string]string)
				}

				_, shardInLargestSeenMap := kv.ShardToLargestSeenClientCommandMap[shardResponsible]

				if shardInLargestSeenMap == false {
					kv.ShardToLargestSeenClientCommandMap[shardResponsible] = make(map[int64]CommandNumberAndValue)
				}

				if currentCommand.ClientCommandNumber > kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID].CommandNumber {

					if currentCommand.OperationType == PUT {

						kv.ShardToKvDict[shardResponsible][currentCommand.Key] = currentCommand.Value
						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: ""}
						kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID] = updateCNV

					} else if currentCommand.OperationType == APPEND {

						currentValue := kv.ShardToKvDict[shardResponsible][currentCommand.Key]
						currentValue += currentCommand.Value
						kv.ShardToKvDict[shardResponsible][currentCommand.Key] = currentValue

						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: ""}
						kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID] = updateCNV

					} else {
						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: kv.ShardToKvDict[shardResponsible][currentCommand.Key]}
						kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID] = updateCNV
					}

					kv.HighestIndexSeen = applyChannelMessage.Index-1 // minus 1 because sent to raft
					kv.HighestTermSeen = applyChannelMessage.Term

					currentIndexChannels := kv.shardToClientCommandShutdownChannelMap[shardResponsible][commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)
							// Create appropriate value to shutdown channel
							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {

								toChannel.Succeeded = true
								if currentCommand.OperationType == GET {
									toChannel.ClientReturnValue = kv.ShardToKvDict[shardResponsible][currentCommand.Key] 
								}
							}
							select{
							case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
							default:
							}							
							break
						}
					}

					kv.deleteCommandChannel(clientCommandChannelIndex, clientID, shardResponsible)
				} else if currentCommand.ClientCommandNumber == kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID].CommandNumber {
					currentIndexChannels := kv.shardToClientCommandShutdownChannelMap[shardResponsible][commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)
							// Create appropriate value to shutdown channel
							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {
								toChannel.Succeeded = true
								toChannel.ClientReturnValue = kv.ShardToLargestSeenClientCommandMap[shardResponsible][clientID].Value
							}
							select{
							case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
							default:
							}							
							break		
						}
					}	
					//Delete from table
					kv.deleteCommandChannel(clientCommandChannelIndex, clientID, shardResponsible)	
				} 

				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
