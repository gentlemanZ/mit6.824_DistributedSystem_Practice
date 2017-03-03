package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string
	// Your declarations here.
	currentView View
	ackView     View
	timeTrack   map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	//check if args.me is in the currentView map.
	//fmt.Print(args.Me)
	//fmt.Println(" is ack with view number:", args.Viewnum)
	vs.timeTrack[args.Me] = time.Now()
	if vs.currentView.Primary == "" {
		//first ping set up the Primary
		vs.currentView.Viewnum++
		vs.currentView.Primary = args.Me
		vs.ackView = vs.currentView
	} else if vs.currentView.Primary == "" && vs.currentView.Viewnum != 0 {
		vs.currentView.Viewnum++
		vs.currentView.Primary = args.Me
		vs.ackView = vs.currentView
	} else if vs.currentView.Primary != args.Me && vs.currentView.Backup == "" {
		// When primary is set and backup is empty, set up the back up.
		if args.Viewnum == 0 {
			vs.currentView.Viewnum++
			vs.currentView.Backup = args.Me
		} else {
			vs.currentView.Backup = args.Me
		}

		//fmt.Println(vs.currentView)
	} else if vs.currentView.Primary == args.Me {
		//handle the case that primary server die and come beack alive at once.
		if args.Viewnum == 0 && args.Viewnum != vs.currentView.Viewnum {
			vs.currentView.Viewnum++
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = args.Me
			//fmt.Print("The prime is dead and come back at onece, give it the ackview: ")
			//fmt.Println(vs.ackView)
			//here since the prime is not prime anymore we should give it ackview.
			var rep PingReply
			rep.View = vs.ackView
			*reply = rep
			return nil
		} else if args.Viewnum != 0 && args.Viewnum != vs.currentView.Viewnum {
			//fmt.Print("The prime's ackview is old give it the currentview: ")
			//fmt.Println(vs.currentView)
			//vs.ackView = vs.currentView
			var rep PingReply
			rep.View = vs.currentView
			*reply = rep
			return nil
		} else if args.Viewnum != 0 && args.Viewnum == vs.currentView.Viewnum {
			//fmt.Print("The prime's ackview is right! update ackview: ")
			//fmt.Println(vs.currentView)
			vs.ackView = vs.currentView
			var rep PingReply
			rep.View = vs.currentView
			*reply = rep
			return nil
		}

	}
	//fmt.Println("return ack view:", vs.ackView)
	var rep PingReply
	rep.View = vs.ackView
	*reply = rep
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	var rep GetReply
	rep.View = vs.currentView
	*reply = rep

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	for key, value := range vs.timeTrack {
		timeDifference := time.Since(value)
		if timeDifference >= DeadPings*PingInterval {
			//the server is dead
			//fmt.Print(key)
			//fmt.Println(" is dead")
			if vs.ackView.Primary == key && vs.ackView == vs.currentView {
				//Prime is dead
				//fmt.Print(key)
				//fmt.Println(" Prime is dead")
				vs.currentView.Viewnum++
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				for key2, value2 := range vs.timeTrack { //find an alive server for back up.
					timeDifference2 := time.Since(value2)
					if timeDifference2 < DeadPings*PingInterval {
						if vs.currentView.Primary != key2 {
							vs.currentView.Backup = key2
						}
					}
				}
			} else if vs.ackView.Primary == key && vs.ackView != vs.currentView {
				//fmt.Print(key)
				//fmt.Println(" Prime is dead but ackview != currentview")
				vs.ackView.Viewnum++
				vs.ackView.Primary = vs.ackView.Backup
				vs.ackView.Backup = ""
				//vs.currentView = vs.ackView
			} else if vs.ackView.Backup == key {
				//backup is dead. remove it.
				//fmt.Print(key)
				//fmt.Println(" backup is dead")
				vs.currentView.Viewnum++
				vs.currentView.Backup = ""
				delete(vs.timeTrack, key)
			}
			//fmt.Println("current view becomes: ", vs.currentView)
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{0, "", ""} //init a new currentView for viewserver.
	vs.ackView = View{0, "", ""}
	vs.timeTrack = make(map[string]time.Time)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
