package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type StageType int 
const (
	MAP_STAGE 	 StageType	= 1
	REDUCE_STAGE StageType 	= 2
	DONE_STAGE	 StageType	= 3
)

type Coordinator struct {
	lock sync.Mutex // protect corrdinator state from cocurrent access
	
	stage          StageType
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

// Worker gets a new task from coordinator
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task, ok := <- c.availableTasks
	if !ok {
		reply.NoAvaliableTask = true
		return nil
	}

	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)
	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce

	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	TaskID := GenTaskID(args.TaskType, args.TaskIndex)
	task, exists := c.tasks[TaskID]
	if exists {
		log.Printf(
			"Mark %s task %d as finished on worker %s\n",
			task.Type, task.Index, args.WorkerID)
		
		if args.TaskType == MAP {
			for ri := 0; ri < c.nReduce; ri++ {
				err := os.Rename(
					tmpMapOutFile(args.WorkerID, args.TaskIndex, ri),
					finalMapOutFile(args.TaskIndex, ri))
				if err != nil {
					log.Fatalf(
						"Failed to mark map output file `%s` as final: %e",
						tmpMapOutFile(args.WorkerID, args.TaskIndex, ri), err)
				}
			}
		} else if args.TaskType == REDUCE {
			err := os.Rename(
				tmpReduceOutFile(args.WorkerID, args.TaskIndex),
				finalReduceOutFile(args.TaskIndex))
			if err != nil {
				log.Fatalf(
					"Failed to mark reduce output file `%s` as final: %e",
					tmpReduceOutFile(args.WorkerID, args.TaskIndex), err)
			}
		}
		delete(c.tasks, TaskID)

		if len(c.tasks) == 0 { // All tasks have been done, enter the next stage
			c.transit()
		}
	}
	
	return nil
}


func (c *Coordinator) transit() {
	if c.stage == MAP_STAGE {
		log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")
		c.stage = REDUCE_STAGE

		for i := 0; i < c.nReduce; i++ {	// Generate REDUCE tasks
			task := Task{
				Type: REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE_STAGE {
		log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks)
		c.stage = DONE_STAGE
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == DONE_STAGE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP_STAGE,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	for i, file := range files {
		task := Task{
			Type: MAP,
			Index: i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	// Collect time out tasks 
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf(
						"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

func GenTaskID(t TaskType, index int) string {
	return fmt.Sprintf("%s-%d", string(t), index)
}