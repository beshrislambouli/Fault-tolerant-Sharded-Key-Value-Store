package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	InputFiles []string
	NReduce int
	MapJobs []int
	MapJobsT []time.Time
	ReduceJobs []int
	ReduceJobsT[]time.Time
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Task(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.M = len (c.InputFiles)
	reply.R = c.NReduce
	t := "map done"
	for i := 0 ; i < len (c.MapJobs) ; i ++ {
		if c.MapJobs [i] == 0 {
			reply.JobType = "map"
			reply.TNum = i
			reply.Filename = c.InputFiles[i]
			c.MapJobs[i] = 1
			c.MapJobsT [i] = time.Now ()
			return nil
		}
		if c.MapJobs [i] == 1 {
			if time.Now().Sub(c.MapJobsT[i]) > 10*time.Second {
				reply.JobType = "map"
				reply.TNum = i
				reply.Filename = c.InputFiles[i]
				c.MapJobs[i] = 1
				c.MapJobsT [i] = time.Now ()
				return nil
			} else {
				t = "map in progress"
			}
		}
	}
	if t == "map in progress" {
		reply.JobType = "wait"
		return nil 
	}
	t = "reduce done"
	for i := 0 ; i < c.NReduce ; i ++ {
		if c.ReduceJobs [i] == 0 {
			reply.JobType = "reduce"
			reply.TNum = i
			c.ReduceJobs [i] = 1
			c.ReduceJobsT [i] = time.Now ()
			return nil
		}
		if c.ReduceJobs [i] == 1 {
			if time.Now().Sub(c.ReduceJobsT[i]) > 10*time.Second {
				reply.JobType = "reduce"
				reply.TNum = i
				c.ReduceJobs [i] = 1
				c.ReduceJobsT [i] = time.Now ()
				return nil
			} else {
				t = "reduce in progress"
			}
		}
	}
	if t == "reduce in progress" {
		 // better not kill the worker becasue we might need it if another one took forever
		reply.JobType = "wait"
	} else {
		reply.JobType = "exit"
	}
	return nil
	// if c.NxtMap < len(c.InputFiles) {
	// 	reply.JobType = "map"
	// 	reply.TNum = c.NxtMap
	// 	reply.Filename = c.InputFiles[c.NxtMap]
	// 	c.NxtMap ++
	// } else if c.NxtReduce < c.NReduce {
	// 	reply.JobType = "reduce"
	// 	reply.TNum = c.NxtReduce
	// 	c.NxtReduce ++
	// } else {
	// 	reply.JobType = "no job"
	// 	c.done = true
	// }
	// return nil
}
func (c *Coordinator) Upd(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.JobType == "map" {
		c.MapJobs [args.TNum] = 2
	} else {
		c.ReduceJobs [args.TNum] = 2
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.

	for i := 0 ; i < c.NReduce ; i ++ {
		if c.ReduceJobs [i] != 2 {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator {
		InputFiles: files,
		NReduce: nReduce,
	}

	// Your code here.
	for i := 0 ; i < len (files) ; i ++ {
		c .MapJobs = append(c.MapJobs, 0)
		c .MapJobsT= append(c.MapJobsT, time.Now())
	}
	for i := 0 ; i < nReduce ; i ++ {
		c .ReduceJobs = append(c.ReduceJobs, 0)
		c .ReduceJobsT= append(c.ReduceJobsT, time.Now())
	}

	c.server()
	return &c
}
