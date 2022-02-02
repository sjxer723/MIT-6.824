package mr

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)
import "log"
import "os"
import "strconv"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func performMap(filename string, WorkerID string, TaskIndex int, nReduceNum int, mapf func(string, string) []KeyValue) {
	// Open and read the contents of the file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open map input file %s: %e", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read map input file %s: %e", filename, err)
	}
	kva := mapf(filename, string(content)) // Get intermediate results
	
	hashedKva := make(map[int][]KeyValue) 
	for _, kv := range kva {
		hashed := ihash(kv.Key) % nReduceNum
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}
	// Write the intermediate results in temporary files
	for i := 0; i < nReduceNum; i++ {
		ofile, _ := os.Create(tmpMapOutFile(WorkerID, TaskIndex, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
}

func performReduce(WorkerID string, TaskIndex int, nMapNum int, reducef func(string, []string) string) {
	var lines []string
	var kva []KeyValue
	
	for mi := 0; mi < nMapNum; mi++ {
		inputFile := finalMapOutFile(mi, TaskIndex)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key: parts[0],
			Value: parts[1],
		})
	}
	sort.Sort(ByKey(kva))

	ofile, _ := os.Create(tmpReduceOutFile(WorkerID, TaskIndex))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()	
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	for { // Enter loop, continue to apply for a new task until no tasks left.
		args := GetTaskArgs{ WorkerID: id}
		reply := GetTaskReply{}
		call("Coordinator.HandleGetTask", &args, &reply)

		if reply.NoAvaliableTask { 	// All tasks have been finished!
			log.Printf("Received job finish signal from coordinator")
			break
		}

		log.Printf("Received %s task %d from coordinator", reply.TaskType, reply.TaskIndex)
		switch reply.TaskType {
		case MAP:
			performMap(reply.MapInputFile, id, reply.TaskIndex, reply.ReduceNum,mapf)
		case REDUCE:
			performReduce(id, reply.TaskIndex, reply.MapNum, reducef)	
		default:
			log.Fatalf("Unknown task type")
		}
		finargs := FinishedTaskArgs {
			WorkerID: id,
			TaskType: reply.TaskType,
			TaskIndex: reply.TaskIndex,
		}
		finreply := FinishedTaskReply {}
		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskIndex)
		call("Coordinator.HandleFinishedTask", &finargs, &finreply)
	}
	log.Printf("Worker %s exit\n", id)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}