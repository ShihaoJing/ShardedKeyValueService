package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex
var nextWorkerID int

type TaskStatus struct {
	workerID int
	task     *TaskType
	runtime  int
}

type Master struct {
	// Your definitions here.
	inputFiles              []string
	nReduceTasks            int
	mapOutputFiles          []string
	taskQueue               []*TaskType
	workerIDToTaskStatusMap map[int]int
	workerIDToTaskNodeMap   map[int]*list.Element
	taskStatusList          *list.List
	taskStatus              []TaskStatus
	tasks                   int
	finishedTasks           []*TaskType
	phase                   int // 0 map 1 reduce
	done                    bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) init() {
	fmt.Printf("Initializing master...\n")
	m.phase = 0
	m.workerIDToTaskStatusMap = make(map[int]int)
	m.tasks = 0
	m.done = false
	m.taskStatusList = list.New()
	taskQueue := []*TaskType{}
	for i, file := range m.inputFiles {
		mapTask := TaskType{i, 0, m.nReduceTasks, []string{file}}
		taskQueue = append(taskQueue, &mapTask)
		m.tasks++
		fmt.Printf("Generating Map Task: %#v\n", mapTask)
	}
	m.taskQueue = taskQueue
	go m.checkProgress()
}

func (m *Master) checkProgress() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		// 1. Check if worker timeouts
		// 2. Check if map/reduce phase is finished
		fmt.Printf("Checking progress: phase: [%v], total tasks: [%v], remaining tasks: [%v], finished Tasks: [%v].\n", m.phase, m.tasks, len(m.taskQueue), len(m.finishedTasks))
		if len(m.finishedTasks) == m.tasks {
			if m.phase == 0 {
				m.taskQueue = []*TaskType{}
				for i := 0; i < m.nReduceTasks; i++ {
					reduceTask := TaskType{i, 1, m.nReduceTasks, []string{}}
					for _, task := range m.finishedTasks {
						reduceTask.InputFiles = append(reduceTask.InputFiles, fmt.Sprintf("mr-%v-%v", task.ID, i))
					}
					m.taskQueue = append(m.taskQueue, &reduceTask)
					fmt.Printf("Generating Reduce Task: %#v\n", reduceTask)
				}
				m.tasks = m.nReduceTasks
				m.finishedTasks = []*TaskType{}
				m.phase = 1
			} else if m.phase == 1 {
				m.finishedTasks = []*TaskType{}
				m.tasks = 0
				m.phase = 2
				m.done = true
			}
		} else {
			for i := range m.taskStatus {
				status := &m.taskStatus[i]
				// Skip obselete task status
				if status.runtime == -1 {
					continue
				}
				status.runtime++
				if status.runtime >= 10 {
					fmt.Printf("Found timeout task: taskID[%v], workerID[%v]\n", status.task.ID, status.workerID)
					fmt.Printf("Enqueue timeout task: %#v\n", status.task)
					status.runtime = -1
					m.taskQueue = append(m.taskQueue, status.task)
				}
			}
			for i := range m.taskStatus {
				status := &m.taskStatus[i]
				// Skip obselete task status
				if status.runtime == -1 {
					continue
				}
				status.runtime++
				if status.runtime >= 10 {
					fmt.Printf("Found timeout task: taskID[%v], workerID[%v]\n", status.task.ID, status.workerID)
					fmt.Printf("Enqueue timeout task: %#v\n", status.task)
					status.runtime = -1
					m.taskQueue = append(m.taskQueue, status.task)
				}
			}
		}
		mu.Unlock()
	}
}

//
// RPC for fetching task
//
func (m *Master) FetchTask(args *FetchTaskRequest, reply *FetchTaskResponse) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("Receiving request: %#v\n", args)
	workerID := args.WorkerID

	if m.phase != 2 {
		if len(m.taskQueue) == 0 {
			reply.Status = 1
			return nil
		}
		// fetch a task
		task := m.taskQueue[0]
		m.taskQueue = m.taskQueue[1:]
		// build reply
		reply.Task = *task
		reply.Status = 0
		// add a worker status
		status := TaskStatus{workerID, task, 0}
		m.taskStatus = append(m.taskStatus, status)
		m.workerIDToTaskStatusMap[workerID] = len(m.taskStatus) - 1

		element := m.taskStatusList.PushBack(status)
		m.workerIDToTaskNodeMap[workerID] = element
		fmt.Printf("Replying: %#v\n", reply)
		return nil
	} else {
		reply.Status = 2
	}
	return nil
}

//
// RPC for reporting task
//
func (m *Master) ReportTask(args *ReportTaskRequest, reply *ReportTaskResponse) error {
	mu.Lock()
	if args.Action == 1 {
		// Task complete
		taskStatus := &m.taskStatus[m.workerIDToTaskStatusMap[args.WorkerID]]
		taskNode := m.workerIDToTaskNodeMap[args.WorkerID]
		if taskNode.Value.(TaskStatus).runtime != -1 {
			m.taskStatusList.Remove(taskNode)
			m.finishedTasks = append(m.finishedTasks, taskNode.Value.(TaskStatus).task)
		}
		// Mark done if task not time out yet
		if taskStatus.runtime != -1 {
			taskStatus.runtime = -1
			m.finishedTasks = append(m.finishedTasks, taskStatus.task)
		}
	}
	mu.Unlock()
	return nil
}

//
// RPC for asigning ID to worker
//
func (m *Master) FetchWorkerID(args *FetchWorkerIDRequest, reply *FetchWorkerIDResponse) error {
	mu.Lock()
	reply.WorkerID = nextWorkerID
	nextWorkerID++
	mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.done {
		fmt.Println("All task finished. Exiting...")
	}
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.inputFiles = files
	m.nReduceTasks = nReduce
	m.init()

	m.server()
	return &m
}
