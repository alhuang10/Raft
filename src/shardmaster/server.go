package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"
import (
  log "github.com/Sirupsen/logrus"
)

func init() {
  // Only log the warning severity or above.
  // log.SetLevel(log.InfoLevel)
  // log.SetLevel(log.DebugLevel)
  // log.SetLevel(log.WarnLevel)
  // log.SetLevel(log.ErrorLevel)
  log.SetLevel(log.FatalLevel)
}

type ClientIDTermChannel struct {
	UniqueClientID int64
	Term 	int
	CommandShutdownChannel chan ShutdownValue
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// type Config struct {
	// 	Num    int              // config number
	// 	Shards [NShards]int     // shard -> gid
	// 	Groups map[int][]string // gid -> servers[]
	// }
	// Copy current config them add it to the log
	currentConfig 	Config

	clientCommandShutdownChannelMap			map[int][]ClientIDTermChannel

	//TODO: some config dictionary or something

	// Just int because we don't need to store a return value, just reject same command unmber if JOIN, LEAVE, or MOVE and respond with config if QUERY because that's just a read
	LargestSeenClientCommandMap		map[int64]int

	configs []Config // indexed by config num

	shardmasterShutdownChannel	chan int // For ending goroutines in Kill()


}


// Operation Enum
const (
	JOIN = 0
	LEAVE = 1
	MOVE = 2
	QUERY = 3
)

const SHUTDOWN_CHANNEL_TIMEOUT = 100

type Op struct {
	UniqueClientID	int64 // from nrand()
	ClientCommandNumber int
	OperationType		int

	// Join
	Servers 	map[int][]string //think you need to duplicate the map before putting it in the raft log

	// Leave
	GIDs 	[]int

	// Move
	Shard 	int
	GID 	int

	// Query
	Num 	int
}

type ShutdownValue struct {

	ClientReturnValue Config
	Succeeded	bool
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {

	// Copy all this shit because maps and slices passed by value
	duplicateServersMapping := make(map[int][]string)
	for index, serverNames := range args.Servers {
		serverNamesCopy := make([]string, len(serverNames))
		copy(serverNamesCopy, serverNames)

		duplicateServersMapping[index] = serverNamesCopy
	}

	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: JOIN, Servers: duplicateServersMapping}

	sm.mu.Lock()
	index, term, isLeader := sm.rf.Start(commandToLog)

	if isLeader {	

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//sm.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand
		
		// Create the shutdown channel struct
		// Check if the index has something there
		// If it does
			// Just append the new struct to the slice
		// If not
			// Make a new slice with that one element in it
		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := sm.clientCommandShutdownChannelMap[index]

		if inMap {
			sm.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			sm.clientCommandShutdownChannelMap[index] = newSlice
		}

		sm.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			sm.mu.Lock()
			sm.deleteCommandChannel(index, args.ClientIDNumber)
			sm.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		sm.mu.Unlock()		
		reply.WrongLeader = true
	}

	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {

	// Copy because slices passed by value
	gidCopy := args.GIDs
	// gidCopy := make([]int, len(args.GIDs))
	// copy(gidCopy, args.GIDs)

	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: LEAVE, GIDs: gidCopy}

	sm.mu.Lock() 
	index, term, isLeader := sm.rf.Start(commandToLog)

	if isLeader {	

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//sm.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand
		
		// Create the shutdown channel struct
		// Check if the index has something there
		// If it does
			// Just append the new struct to the slice
		// If not
			// Make a new slice with that one element in it
		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := sm.clientCommandShutdownChannelMap[index]

		if inMap {
			sm.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			sm.clientCommandShutdownChannelMap[index] = newSlice
		}

		sm.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			sm.mu.Lock()
			sm.deleteCommandChannel(index, args.ClientIDNumber)
			sm.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		sm.mu.Unlock()		
		reply.WrongLeader = true
	}

	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	
	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: MOVE, Shard: args.Shard , GID: args.GID}

	sm.mu.Lock() 
	index, term, isLeader := sm.rf.Start(commandToLog)

	if isLeader {	

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//sm.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand
		
		// Create the shutdown channel struct
		// Check if the index has something there
		// If it does
			// Just append the new struct to the slice
		// If not
			// Make a new slice with that one element in it
		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := sm.clientCommandShutdownChannelMap[index]

		if inMap {
			sm.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			sm.clientCommandShutdownChannelMap[index] = newSlice
		}

		sm.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			sm.mu.Lock()
			sm.deleteCommandChannel(index, args.ClientIDNumber)
			sm.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		sm.mu.Unlock()		
		reply.WrongLeader = true
	}

	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	
	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: QUERY, Num: args.Num}

	sm.mu.Lock() 
	index, term, isLeader := sm.rf.Start(commandToLog)

	if isLeader {	

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//sm.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand
		
		// Create the shutdown channel struct
		// Check if the index has something there
		// If it does
			// Just append the new struct to the slice
		// If not
			// Make a new slice with that one element in it
		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := sm.clientCommandShutdownChannelMap[index]

		if inMap {
			sm.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			sm.clientCommandShutdownChannelMap[index] = newSlice
		}

		sm.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			sm.mu.Lock()
			sm.deleteCommandChannel(index, args.ClientIDNumber)
			sm.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				reply.Config = shutdownValue.ClientReturnValue // will be a Config struct if it's a Query
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		sm.mu.Unlock()		
		reply.WrongLeader = true
	}

	return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.shardmasterShutdownChannel)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) deleteCommandChannel(index int, clientID int64) {

	// Iterate through the slice mapped to by the provided index until you get to the ClientIDTermChannel struct that has a matching clientID
	// Keep track of which index of the slice you're on and remove from
	indexToDelete := -1

	for index, element := range sm.clientCommandShutdownChannelMap[index] {
		if element.UniqueClientID == clientID {
			indexToDelete = index
		}
	}

	// Two sources of deletion, one from timeout and one if another command arrived at that same index
	if indexToDelete != -1 {
		currentSlice := sm.clientCommandShutdownChannelMap[index]
		sm.clientCommandShutdownChannelMap[index] = append(currentSlice[:indexToDelete], currentSlice[indexToDelete+1:]...)
	}
}


// type Config struct {
// 	Num    int              // config number
// 	Shards [NShards]int     // shard -> gid
// 	Groups map[int][]string // gid -> servers[]
// }

// Copy current config them add it to the log
func (sm *ShardMaster) addCurrentConfigToConfigLog() {


	// Copy the shards field
	// shardsCopy := sm.currentConfig.Shards // this just copies in go b/c it's an array
	// copy(shardsCopy, sm.currentConfig.Shards)

	groupsCopy := make(map[int][]string)
	for index, serverNames := range sm.currentConfig.Groups {

		serverNamesCopy := make([]string, len(serverNames))
		copy(serverNamesCopy, serverNames)
		groupsCopy[index] = serverNamesCopy
	}

	// log.WithFields(log.Fields{"Num": sm.currentConfig.Num, 
	// 	"Shards": sm.currentConfig.Shards,}).Debug("Adding to configs")

	configCopy := Config{Num: sm.currentConfig.Num, Shards: sm.currentConfig.Shards, Groups: groupsCopy}

	sm.configs = append(sm.configs, configCopy)
}

func (sm *ShardMaster) numShardsThisGIDHas(GID int) int {

	count := 0
	for _, gidForShard := range sm.currentConfig.Shards {
		if GID == gidForShard {
			count = count+1
		}
	}
	return count
}


func (sm *ShardMaster) getFewestShardsGID() int {

	fewestGID := -1
	numShardsInFewestGID := 0

	for gid := range sm.currentConfig.Groups {
		if fewestGID == -1 {
			fewestGID = gid
			numShardsInFewestGID = sm.numShardsThisGIDHas(gid)
		} else {
			ownedShards := sm.numShardsThisGIDHas(gid)
			if ownedShards < numShardsInFewestGID {
				fewestGID = gid
				numShardsInFewestGID = ownedShards
			}
		}
	}

	return fewestGID
}

func (sm *ShardMaster) getMostShardsGID() int {
	
	largestGID := -1
	numShardsInLargestGID := -1

	for gid := range sm.currentConfig.Groups {
		
		ownedShards := sm.numShardsThisGIDHas(gid)
		if ownedShards > numShardsInLargestGID {
			largestGID = gid
			numShardsInLargestGID = ownedShards
		}
	}

	return largestGID
}

// Rebalances shards to GIDs using minimum number of transfers
// For when there is more than 1 GID
func (sm *ShardMaster) rebalanceShards() {

	// lowerThreshold := NShards/len(sm.currentConfig.Groups)
	// upperThreshold := NShards/len(sm.currentConfig.Groups) + 1

	fewestGID := sm.getFewestShardsGID()
	highestGID := sm.getMostShardsGID()

	// log.Info(len(sm.currentConfig.Groups))
	// log.WithFields(log.Fields{ "lowerThreshold": NShards/len(sm.currentConfig.Groups), "upperThreshold": NShards/len(sm.currentConfig.Groups) + 1, }).Info("Rebalancing shards")

	// for (sm.numShardsThisGIDHas(fewestGID) < lowerThreshold || sm.numShardsThisGIDHas(highestGID) > upperThreshold) {
	for ((sm.numShardsThisGIDHas(highestGID)-sm.numShardsThisGIDHas(fewestGID)) > 1) {

		// log.WithFields(log.Fields{"fewestGID": fewestGID, 
		// 	"fewestAmount": sm.numShardsThisGIDHas(fewestGID),
		// 	"highestGID": highestGID, 
		// 	"highestAmount": sm.numShardsThisGIDHas(highestGID), }).Info("Not balanced yet")
		
		// While this is true, transfer a GID from highest to lowest
		for shard, gidForShard := range sm.currentConfig.Shards{
			if gidForShard == highestGID {
				sm.currentConfig.Shards[shard] = fewestGID
				break
			}
		}
		fewestGID = sm.getFewestShardsGID()
		highestGID = sm.getMostShardsGID()
	}

	log.Debug(sm.currentConfig.Shards)
	// log.Info("Finished Rebalancing")
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	var initialShardCopy [NShards]int
	var initialShardCopy2 [NShards]int

	sm.configs = make([]Config, 1)
	sm.configs[0].Shards = initialShardCopy
	sm.configs[0].Groups = map[int][]string{}
	

	// log.Debug(sm.configs[0].Shards)
	sm.currentConfig = Config{}
	sm.currentConfig.Num = 0
	sm.currentConfig.Shards = initialShardCopy2

	// sm.currentConfig.Shards = [NShards]int
	sm.currentConfig.Groups = make(map[int][]string)

	gob.Register(Op{})
	gob.Register(Config{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.LargestSeenClientCommandMap = make(map[int64]int)
	sm.clientCommandShutdownChannelMap = make(map[int][]ClientIDTermChannel)

	sm.shardmasterShutdownChannel = make(chan int)

	go func() {
		for {
			select{
			case <- sm.shardmasterShutdownChannel:
				return
			case applyChannelMessage := <- sm.applyCh:

				sm.mu.Lock()

				currentCommand := applyChannelMessage.Command.(Op)
				clientID := currentCommand.UniqueClientID
				commandIndex := applyChannelMessage.Index
				currentTerm := sm.rf.CurrentTerm

				if currentCommand.ClientCommandNumber > sm.LargestSeenClientCommandMap[clientID] {

					if currentCommand.OperationType == QUERY {
						// Pass because no modification so skip adding new config
					} else {

						if currentCommand.OperationType == JOIN {

							// log.Debug(sm.currentConfig.Num)
							// Update the groups table
							for GID, serverNames := range currentCommand.Servers {
								sm.currentConfig.Groups[GID] = serverNames
							}
							// Transfer shards to the new GID so it's distributed evenly
							// TODO: efficient intitial distributions
							if sm.currentConfig.Num == 0 {
								for GID,_ := range sm.currentConfig.Groups {
									for shard,_ := range sm.currentConfig.Shards {
										sm.currentConfig.Shards[shard] = GID
									}
									break
									// Break because we just want to 
								}
							} 
							sm.rebalanceShards()

						} else if currentCommand.OperationType == LEAVE {

							shardsToReassign := make([]int, 0)

							for _, toDeleteGID := range currentCommand.GIDs {
								delete(sm.currentConfig.Groups, toDeleteGID)

								for shardNumber, shardGID := range sm.currentConfig.Shards {
									if shardGID == toDeleteGID {
										shardsToReassign = append(shardsToReassign, shardNumber)
									}
								}
							}
							//assign each one to the least value
							for _, shard := range shardsToReassign {
								fewestGID := sm.getFewestShardsGID()
								sm.currentConfig.Shards[shard] = fewestGID
							}
							// That should be enough to rebalance it

							// Give all the shards that have no GID anymore to the remaining GIDs (keys of Groups)
							// sm.rebalanceShards()

						} else if currentCommand.OperationType == MOVE {

							sm.currentConfig.Shards[currentCommand.Shard] = currentCommand.GID
						}

						// Increase config number by 1
						sm.currentConfig.Num = sm.currentConfig.Num + 1
						// Adds the new config to the config log
						sm.addCurrentConfigToConfigLog()
					}

					currentIndexChannels := sm.clientCommandShutdownChannelMap[commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)

							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {
								toChannel.Succeeded = true
								if currentCommand.OperationType == QUERY {

									ConfigCopy := Config{}
									// if negative or higher return the latest config
									if currentCommand.Num == -1 || currentCommand.Num >= len(sm.configs) {
										// copy the current config
										configWeWant := sm.configs[len(sm.configs)-1]

										shardsCopy := configWeWant.Shards
										// shardsCopy := make([]int, NShards)
										// copy(shardsCopy, configWeWant.Shards)

										groupsCopy := make(map[int][]string)
										for index, serverNames := range configWeWant.Groups {
											serverNamesCopy := make([]string, len(serverNames))
											copy(serverNamesCopy, serverNames)
											groupsCopy[index] = serverNamesCopy
										}

										ConfigCopy = Config{Num: configWeWant.Num, Shards: shardsCopy, Groups: groupsCopy}

									} else {
										// return the corresponding config
										configWeWant := sm.configs[currentCommand.Num]

										shardsCopy := configWeWant.Shards
										// shardsCopy := make([]int, NShards)
										// copy(shardsCopy, configWeWant.Shards)

										groupsCopy := make(map[int][]string)
										for index, serverNames := range configWeWant.Groups {
											serverNamesCopy := make([]string, len(serverNames))
											copy(serverNamesCopy, serverNames)
											groupsCopy[index] = serverNamesCopy
										}

										ConfigCopy = Config{Num: configWeWant.Num, Shards: shardsCopy, Groups: groupsCopy}
									}

									toChannel.ClientReturnValue = ConfigCopy
								}
							}

							select{
							case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
							default:
							}							
							break
						}
					}

					sm.deleteCommandChannel(clientCommandChannelIndex, clientID)

				} else if currentCommand.ClientCommandNumber == sm.LargestSeenClientCommandMap[clientID] {
					// Don't repeat a command if it's already been done but answer queries again

					currentIndexChannels := sm.clientCommandShutdownChannelMap[commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)

							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {

								toChannel.Succeeded = true

								if currentCommand.OperationType == QUERY {

									ConfigCopy := Config{}
									// if negative or higher return the latest config
									if currentCommand.Num == -1 || currentCommand.Num >= len(sm.configs) {
										// copy the current config
										configWeWant := sm.currentConfig

										shardsCopy := sm.currentConfig.Shards
										// shardsCopy := make([]int, NShards)
										// copy(shardsCopy, configWeWant.Shards)

										groupsCopy := make(map[int][]string)
										for index, serverNames := range configWeWant.Groups {
											serverNamesCopy := make([]string, len(serverNames))
											copy(serverNamesCopy, serverNames)
											groupsCopy[index] = serverNamesCopy
										}

										ConfigCopy = Config{Num: configWeWant.Num, Shards: shardsCopy, Groups: groupsCopy}

									} else {
										// return the corresponding config
										configWeWant := sm.configs[currentCommand.Num]

										shardsCopy := sm.currentConfig.Shards
										// shardsCopy := make([]int, NShards)
										// copy(shardsCopy, configWeWant.Shards)

										groupsCopy := make(map[int][]string)
										for index, serverNames := range configWeWant.Groups {
											serverNamesCopy := make([]string, len(serverNames))
											copy(serverNamesCopy, serverNames)
											groupsCopy[index] = serverNamesCopy
										}

										ConfigCopy = Config{Num: configWeWant.Num, Shards: shardsCopy, Groups: groupsCopy}
									}

									toChannel.ClientReturnValue = ConfigCopy
								}

								select{
								case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
								default:
								}							
								break
							}
						}
					}
					sm.deleteCommandChannel(clientCommandChannelIndex, clientID)
				}

				sm.mu.Unlock()

			}
		}
	}()

	return sm
}
