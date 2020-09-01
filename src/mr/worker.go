package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	req := FetchWorkerIDRequest{}
	rsp := FetchWorkerIDResponse{}
	fmt.Println("Fectching WorkerID")
	sendRequest("Master.FetchWorkerID", &req, &rsp)
	fmt.Printf("Receiving WorkerID: %#v\n", rsp)
	// uncomment to send the Example RPC to the master.
	Run(rsp.WorkerID, mapf, reducef)

}

func runMap(mapf func(string, string) []KeyValue, task TaskType) error {
	fmt.Println("Running map function")

	intermediate := []KeyValue{}
	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	outputFiles := []*os.File{}
	for i := 0; i < task.NumReduce; i++ {
		ofile, _ := os.Create(fmt.Sprintf("mr-%v-%v", task.ID, i))
		outputFiles = append(outputFiles, ofile)
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.NumReduce
		enc := json.NewEncoder(outputFiles[index])
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}

	fmt.Println("Map finished")
	return nil
}

func runReduce(reducef func(string, []string) string, task TaskType) error {
	intermediate := []KeyValue{}
	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", task.ID))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	fmt.Println("Reduce finished")
	return nil
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func Run(workerID int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		time.Sleep(500 * time.Millisecond)
		// Fetch a task
		args := FetchTaskRequest{workerID}
		reply := FetchTaskResponse{}
		fmt.Printf("Fectching Task: %#v\n", args)
		sendRequest("Master.FetchTask", &args, &reply)
		fmt.Printf("Receiving Task: %#v\n", reply)

		if reply.Status == 0 {
			fmt.Println("Executing Task.")
			jobFinished := false
			task := reply.Task
			if task.Type == 0 {
				if runMap(mapf, task) == nil {
					jobFinished = true
				}
			} else {
				if runReduce(reducef, task) == nil {
					jobFinished = true
				}
			}
			// Report back
			if jobFinished {
				fmt.Println("Task finished. Reporting back to master.")
				args := ReportTaskRequest{workerID, 1}
				reply := ReportTaskResponse{}
				sendRequest("Master.ReportTask", &args, &reply)
			}
		}

		if reply.Status == 1 {
			fmt.Println("No task available. Sleeping for one second...")
			time.Sleep(time.Second)
		}

		if reply.Status == 2 {
			fmt.Println("All task finished. Exiting...")
			break
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func sendRequest(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
