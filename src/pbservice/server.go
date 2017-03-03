package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	currentView      viewservice.View
	keyValues        map[string]string
	putDuplicated    map[string]bool
	appendDuplicated map[string]bool
	
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.currentView.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else {
		reply.Err = OK
		reply.Value = pb.keyValues[args.Key]
	}
	//fmt.Println(pb.me)
	//fmt.Println("Get", args, reply)
	//fmt.Println(pb.keyValues)
	pb.mu.Unlock()
	return nil

}
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	//fmt.Println(pb.me)
	//fmt.Print("key values:", pb.keyValues)
	//fmt.Println(" putduplicated :", pb.putDuplicated)
	if args.Primary == "" && pb.currentView.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else if args.Op == "Put" {
		reply.Err = OK
		if pb.putDuplicated[args.Value] {
			pb.mu.Unlock()
			return nil
		} else {
			pb.keyValues[args.Key] = args.Value
			pb.putDuplicated[args.Value] = true
		}
	} else if args.Op == "Append" {
		reply.Err = OK
		if pb.appendDuplicated[args.Value] {
			pb.mu.Unlock()
			return nil
		} else {
			elem, ok := pb.keyValues[args.Key]
			if ok {
				pb.keyValues[args.Key] = elem + args.Value
				pb.appendDuplicated[args.Value] = true

			} else {
				pb.keyValues[args.Key] = args.Value
				pb.appendDuplicated[args.Value] = true

			}
		}

	}
	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	//fmt.Print("the server is:", pb.me)
	//fmt.Println("  the viewnum is:", pb.currentView.Viewnum)
	pb.mu.Lock()
	vx, err := pb.vs.Ping(pb.currentView.Viewnum)
	//fmt.Println("vx is:", vx.Backup)
	//fmt.Println("current is:", pb.currentView.Backup)
	if pb.me == vx.Primary && !(pb.me == pb.currentView.Primary && vx.Backup != "" && vx.Backup != pb.currentView.Backup && err == nil) {
		pb.currentView = vx
		//fmt.Println("vx is:", vx.Backup)
		//fmt.Println("current is:", pb.currentView.Backup)
		if pb.currentView.Backup != "" {
			var args ForwardArgs
			args.KeyValues = pb.keyValues
			args.PutDuplicated = pb.putDuplicated
			args.AppendDuplicated = pb.appendDuplicated
			var reply ForwardReply
			ok := call(pb.currentView.Backup, "PBServer.Forward", &args, &reply)
			if ok && reply.Err == OK {
				pb.mu.Unlock()
				return
			}
		}
	}
	pb.currentView = vx
	pb.mu.Unlock()

}

//this is a new handler that forward the changed view to backup. @2/18
func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {

	//fmt.Println("I am called!")
	pb.mu.Lock()
	if pb.currentView.Backup != pb.me {
		reply.Err = ErrWrongServer
	} else {
		pb.keyValues = args.KeyValues
		pb.putDuplicated = args.PutDuplicated
		pb.appendDuplicated = args.AppendDuplicated
		reply.Err = OK
	}
	//fmt.Println("Receive", args, reply)
	pb.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currentView = viewservice.View{0, "", ""}
	pb.keyValues = make(map[string]string)
	pb.putDuplicated = make(map[string]bool)
	pb.appendDuplicated = make(map[string]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
