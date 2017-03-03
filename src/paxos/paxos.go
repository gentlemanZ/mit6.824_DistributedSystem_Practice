package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "strconv"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	// Your data here.
	prepareNum  map[string]int
	acceptedNum map[string]int
	fate        map[string]Fate
	value       map[string]interface{}
	maxSeq      int
	peersMaxSeq []int
	doneSeq     int
	peerMinSeq  int
}

//different rpc args struct{}:

type PrepareArgs struct {
	Seq int
	Num int
}

type AcceptArgs struct {
	Seq   int
	Num   int
	Value interface{}
}

type DecidedArgs struct {
	Seq    int
	Num    int
	Value  interface{}
	Source int
	MaxSeq int
}

//different rpc replis struct{}:
type PrepareReply struct {
	AcceptedNumber int
	AcceptedValue  interface{}
}

type AcceptReply struct {
	PrepareNumber int
}

type DecidedReply struct{}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) Start(seq int, v interface{}) {
	go px.propose(seq, v)
}

func (px *Paxos) propose(seq int, v interface{}) {
	// Your code here.
	Seq := strconv.Itoa(seq)
	N := px.getN(seq)
	for !(px.fate[Seq] == Decided) {
		//preparing
		newV, success := px.Prepare(seq, N)
		if !success {
			time.Sleep(time.Millisecond * (50 + time.Duration(rand.Int()%100))) // could have a simpler version.
			continue
		}
		if newV != nil {
			v = newV
		}
		//accepting
		maxPrepareNum, success := px.Accept(seq, N, v)
		if !success {
			N = maxPrepareNum + 1
			time.Sleep(time.Millisecond * (50 + time.Duration(rand.Int()%100)))
			continue
		}
		px.updateMaxSeq(seq)
		//deciding
		go px.Decide(seq, N, v)
		return
	}

}
func (px *Paxos) updateMaxSeq(seq int) {
	if seq != px.maxSeq+1 {
		return
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	var i int
	for i = seq; ; i++ {
		Seq := strconv.Itoa(i)
		item, existed := px.fate[Seq]
		if !existed || item != Decided {
			break
		}
	}
	px.maxSeq = i - 1
}

func (px *Paxos) updateMinPeerSeq(source int, maxSeq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.peersMaxSeq[source] >= maxSeq {
		return
	}

	px.peersMaxSeq[source] = maxSeq
	peerMinSeq := maxSeq
	for _, peerMaxSeq := range px.peersMaxSeq {
		if peerMaxSeq < peerMinSeq {
			peerMinSeq = peerMaxSeq
		}
	}
	if px.peerMinSeq < peerMinSeq {
		for i := px.peerMinSeq; i <= peerMinSeq; i++ {
			I := strconv.Itoa(i)
			delete(px.prepareNum, I)
			delete(px.acceptedNum, I)
			delete(px.value, I)
			delete(px.fate, I)
		}
		px.peerMinSeq = peerMinSeq
	}
}

func (px *Paxos) getN(seq int) int {
	px.mu.Lock()
	defer px.mu.Unlock()
	Seq := strconv.Itoa(seq)
	var N int
	item, existed := px.prepareNum[Seq]
	if existed {
		N = item + 1
	} else {
		N = 1
	}
	return N
}

//this decide and decidehandler:
func (px *Paxos) Decide(seq int, num int, v interface{}) {
	args := &DecidedArgs{}
	args.Seq = seq
	args.Num = num
	args.Value = v
	args.Source = int(px.me)
	//args.MaxSeq = px.maxDoneSeq() // this is not done yet.
	var mustDecidedFunc func(i int)
	mustDecidedFunc = func(i int) {
		var reply DecidedReply
		if i == px.me {
			px.DecideHandler(args, &reply)
			return
		}
		if ok := call(px.peers[i], "Paxos.DecideHandler", args, &reply); ok {
			return
		} else {
			time.Sleep(time.Millisecond * (50 + time.Duration(rand.Int()%100)))
			go mustDecidedFunc(i)
		}
	}
	for i := range px.peers {
		go mustDecidedFunc(i)
	}
}

func (px *Paxos) DecideHandler(args *DecidedArgs, reply *DecidedReply) error {
	// log.Printf("[%d] decided, num: %s, seq %d, val: %v\n", px.me, args.Num, args.Seq, args.Value)
	px.mu.Lock()
	Seq := strconv.Itoa(args.Seq)
	px.prepareNum[Seq] = args.Num
	px.acceptedNum[Seq] = args.Num
	px.value[Seq] = args.Value
	px.fate[Seq] = Decided
	px.mu.Unlock()
	//update the maxSep:
	px.updateMaxSeq(args.Seq)

	px.updateMinPeerSeq(args.Source, args.MaxSeq)
	return nil
}

//this accept and accepthandler:
func (px *Paxos) Accept(seq int, num int, v interface{}) (prepareNum int, success bool) {
	success = true
	majority := len(px.peers)/2 + 1
	prepareNum = num
	var acceptedAcceptNum int
	args := &AcceptArgs{}
	args.Seq = seq
	args.Num = num
	args.Value = v
	replyChan := make(chan *AcceptReply)

	acceptFunc := func(i int) {
		var reply AcceptReply
		if i == int(px.me) {
			px.AcceptHandle(args, &reply)
			replyChan <- &reply
			return
		}
		if ok := call(px.peers[i], "Paxos.AcceptHandle", args, &reply); ok {
			replyChan <- &reply
		} else {
			replyChan <- nil
		}

	}
	for i := range px.peers {
		go acceptFunc(i)
	}

	for _ = range px.peers {
		reply := <-replyChan
		if reply != nil {
			acceptedAcceptNum++
			if prepareNum < reply.PrepareNumber {
				prepareNum = reply.PrepareNumber
				success = false
			}
		}
	}

	// log.Printf("prepare phase finish num: %d/%d, success: %v\n", acceptedAcceptNum, len(px.peers), success)
	success = success && (acceptedAcceptNum >= majority)
	return

}

func (px *Paxos) AcceptHandle(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	Seq := strconv.Itoa(args.Seq)
	prepareNumber, existed := px.prepareNum[Seq]
	if !existed {
		px.fate[Seq] = Pending
		px.prepareNum[Seq] = args.Num
		px.acceptedNum[Seq] = 0
		px.value[Seq] = nil
	} else {
		if args.Num < prepareNumber {
			reply.PrepareNumber = prepareNumber
		} else {
			px.prepareNum[Seq] = args.Num
			px.acceptedNum[Seq] = args.Num
			px.value[Seq] = args.Value
		}
	}
	return nil
}

func (px *Paxos) Prepare(seq int, num int) (v interface{}, success bool) {
	majority := len(px.peers)/2 + 1
	prepareArgs := &PrepareArgs{}
	prepareArgs.Num = num
	prepareArgs.Seq = seq
	var prepare_ok int
	prepareReplyChan := make(chan *PrepareReply)
	//here i am , pick up tomorrow 2/26. this is broadcastprepare
	prepareFunc := func(i int) {
		var reply PrepareReply
		if i == int(px.me) {
			px.PrepareHandle(prepareArgs, &reply)
			prepareReplyChan <- &reply
			return
		}
		if ok := call(px.peers[i], "Paxos.PrepareHandle", prepareArgs, &reply); ok {
			prepareReplyChan <- &reply
		} else {
			prepareReplyChan <- nil
		}

	}
	for i := range px.peers {
		go prepareFunc(i)
	}
	for _ = range px.peers {
		reply := <-prepareReplyChan
		if reply != nil {
			prepare_ok++
		}
	}
	if prepare_ok >= majority {
		success = true
	} else {
		success = false
	}
	return
}

func (px *Paxos) PrepareHandle(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	Seq := strconv.Itoa(args.Seq)
	acceptedNumber, existed := px.acceptedNum[Seq]
	if !existed {
		px.fate[Seq] = Pending
		px.prepareNum[Seq] = args.Num
		px.acceptedNum[Seq] = 0
		px.value[Seq] = nil
	} else {
		if px.acceptedNum[Seq] != 0 {
			reply.AcceptedNumber = acceptedNumber
			reply.AcceptedValue = px.value[Seq]
		} else if px.prepareNum[Seq] < args.Num {
			px.prepareNum[Seq] = args.Num
			px.fate[Seq] = Pending
		}
	}
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if seq > px.doneSeq {
		px.doneSeq = seq
	}
}

func (px *Paxos) Max() int {
	// Your code here.
	return px.maxSeq
}

func (px *Paxos) Min() int {
	// You code here.
	return px.peerMinSeq + 1
}

func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	Seq := strconv.Itoa(seq)
	if Fate, existed := px.fate[Seq]; !existed {
		return Pending, nil
	} else {
		return Fate, px.value[Seq]
	}
}

func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.prepareNum = make(map[string]int)
	px.acceptedNum = make(map[string]int)
	px.fate = make(map[string]Fate)
	px.value = make(map[string]interface{})
	px.peersMaxSeq = make([]int, len(px.peers))
	for i, _ := range px.peersMaxSeq {
		px.peersMaxSeq[i] = -1
	}
	px.maxSeq = -1
	px.doneSeq = -1
	px.peerMinSeq = -1
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
