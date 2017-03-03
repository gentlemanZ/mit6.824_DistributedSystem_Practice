package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "time"
import "syscall"
import "encoding/gob"
import "math/rand"
import crand "crypto/rand"
import "math/big"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	configNum int
	confirmed int
}

const (
	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type Op struct {
	Type    string
	Token   int64
	GID     int64
	Servers []string
	Shard   int
	Num     int
}

func (sm *ShardMaster) AriseOp(op Op) {
	op.Token = nrand()//unique id for this op
	for {
		sm.px.Start(sm.confirmed+1, op)//start all the ops
		to := 10 * time.Millisecond
		for {
			fate, value := sm.px.Status(sm.confirmed + 1)
			if fate == paxos.Decided {
				sm.ConfirmOp(value.(Op))
				if value.(Op).Token == op.Token {
					return
				} else {
					break
				}
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
}

func (sm *ShardMaster) LastestConfig() *Config {
	return &sm.configs[sm.configNum]
}

func (sm *ShardMaster) NewConfig() *Config {
	config := sm.LastestConfig()
	var newConfig Config
	newConfig.Num = config.Num + 1
	newConfig.Shards = [NShards]int64{}
	for shard, gid := range config.Shards {
		newConfig.Shards[shard] = gid
	}
	newConfig.Groups = make(map[int64][]string)
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = []string{}
		for _, server := range servers {
			newConfig.Groups[gid] = append(newConfig.Groups[gid], server)
		}
	}
	sm.configNum++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.configNum]
}

func (sm *ShardMaster) GetGroupShards() map[int64][]int {
	shards := make(map[int64][]int)
	for gid, _ := range sm.LastestConfig().Groups {
		shards[gid] = []int{}
	}
	for shard, gid := range sm.LastestConfig().Shards {
		shards[gid] = append(shards[gid], shard)
	}
	return shards
}

func (sm *ShardMaster) Rebalance(groupShards map[int64][]int, freeShards []int) {
	config := sm.LastestConfig()
	if len(config.Groups) == 0 {
		for idx, _ := range config.Shards {
			config.Shards[idx] = 0
		}
		return
	}
	invalidShards, exists := groupShards[0]
	if exists {
		for _, shard := range invalidShards {
			freeShards = append(freeShards, shard)
		}
		delete(groupShards, 0)
	}
	aveNum := NShards / len(config.Groups)
	aboveNum := NShards % len(config.Groups)
	//fmt.Println("[Rebalancing]", config, groupShards, freeShards, aveNum, aboveNum)
	for _, shards := range groupShards {
		bottomLine := aveNum
		if aboveNum > 0 {
			bottomLine++
		}
		if len(shards) >= bottomLine {
			for i := bottomLine; i < len(shards); i++ {
				freeShards = append(freeShards, shards[i])
			}
			aboveNum--
		}
	}
	idx := 0
	for gid, shards := range groupShards {
		topLine := aveNum
		if aboveNum > 0 {
			topLine++
		}
		if len(shards) < topLine {
			for i := len(shards); i < topLine; i++ {
				config.Shards[freeShards[idx]] = gid
				idx++
			}
			aboveNum--
		}
	}
	//fmt.Println("[Balanced]", config.Shards)
}

func (sm *ShardMaster) ConfirmOp(op Op) {
	switch op.Type {
	case OpJoin:
		_, exists := sm.LastestConfig().Groups[op.GID]
		if !exists {
			config := sm.NewConfig()
			config.Groups[op.GID] = op.Servers
			groupShards := sm.GetGroupShards()
			groupShards[op.GID] = []int{}
			freeShards := []int{}
			sm.Rebalance(groupShards, freeShards)
		}
	case OpLeave:
		_, exists := sm.LastestConfig().Groups[op.GID]
		if exists {
			config := sm.NewConfig()
			delete(config.Groups, op.GID)
			groupShards := sm.GetGroupShards()
			freeShards := groupShards[op.GID]
			delete(groupShards, op.GID)
			sm.Rebalance(groupShards, freeShards)
		}
	case OpMove:
		config := sm.NewConfig()
		config.Shards[op.Shard] = op.GID
	case OpQuery:
	}
	sm.confirmed++
	sm.px.Done(sm.confirmed)
	//fmt.Println("This is the CONFIRM:", sm.me, sm.confirmed, op)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Println("This is the JOIN:", sm.me, args)
	var op Op
	op.Type = OpJoin
	op.GID = args.GID
	op.Servers = args.Servers
	sm.AriseOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Println("This is the LEAVE:", sm.me, args)
	var op Op
	op.Type = OpLeave
	op.GID = args.GID
	sm.AriseOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Println("this is the MOVE:", sm.me, args)
	var op Op
	op.Type = OpMove
	op.Shard = args.Shard
	op.GID = args.GID
	sm.AriseOp(op)
	//fmt.Println("i get here")
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//fmt.Println("This is the QUERY:", sm.me, args)
	var op Op
	op.Type = OpQuery
	op.Num = args.Num
	sm.AriseOp(op)
	if args.Num == -1 || args.Num > sm.configNum {
		reply.Config = *sm.LastestConfig()
	} else {
		reply.Config = sm.configs[args.Num]
	}
	//fmt.Println("This is the REPLY:", sm.me, reply)
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.configNum = 0
	sm.confirmed = -1

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}