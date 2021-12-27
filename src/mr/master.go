package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// states for a given task: idle, busy, completed
const (
	IDLE = iota // 0, meaning is default value when master is initialized
	BUSY
	COMPLETED
)

type Master struct {
	// Your definitions here.
	inputFiles        []string
	mapTaskStates     []int
	reduceTaskStates  []int
	intermediateFiles [][]string
	outputFiles       []string
	mapTaskTimers     []int
	reduceTaskTimers  []int
	mapCompleted      bool
	reduceCompleted   bool
	mu                *sync.Mutex
	// keep track of the time for each task (map or reduce)
	// mapTaskTimes      []time.Time
	// reduceTaskTimes   []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.MapTaskNum = -1
	reply.ReduceTaskNum = -1

	m.mu.Lock()
	if m.mapCompleted {
		for i, v := range m.reduceTaskStates {
			if v == IDLE {
				reply.MapTaskNum = -1
				reply.ReduceTaskNum = i
				reply.Filenames = m.intermediateFiles[i]
				m.reduceTaskStates[i] = BUSY
				break
			}
		}
	} else {
		// if there are map tasks that are idle, return the first one
		for i, v := range m.mapTaskStates {
			if v == IDLE {
				reply.MapTaskNum = i
				reply.ReduceTaskNum = -1
				reply.Filenames = append(reply.Filenames, m.inputFiles[i])
				reply.NReduce = len(m.reduceTaskStates)
				m.mapTaskStates[i] = BUSY
				break
			}
		}
	}
	m.mu.Unlock()

	fmt.Println("END GetTask: ", reply)
	return nil
}

// ignore if already completed the task
func (m *Master) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	reply.AlreadyCompleted = true

	m.mu.Lock()
	if args.status == -1 {
		if args.MapTaskNum != -1 {
			m.mapTaskStates[args.MapTaskNum] = IDLE
		} else if args.ReduceTaskNum != -1 {
			m.reduceTaskStates[args.ReduceTaskNum] = IDLE
		}
	} else if args.MapTaskNum != -1 && m.mapTaskStates[args.MapTaskNum] != COMPLETED {
		reply.AlreadyCompleted = false
		m.mapTaskStates[args.MapTaskNum] = COMPLETED
		for i, v := range args.Filenames {
			m.intermediateFiles[i] = append(m.intermediateFiles[i], v)
		}
	} else if args.ReduceTaskNum != -1 && m.reduceTaskStates[args.ReduceTaskNum] != COMPLETED {
		reply.AlreadyCompleted = false
		m.reduceTaskStates[args.ReduceTaskNum] = COMPLETED
		m.outputFiles[args.ReduceTaskNum] = args.Filenames[0]
	}
	m.mu.Unlock()

	m.CheckMapReduceCompleted()

	fmt.Println("TaskCompleted: ", args, reply, m.mapCompleted, m.reduceCompleted)
	return nil
}

func (m *Master) CheckMapReduceCompleted() {
	m.mu.Lock()
	mapsAllCompleted := true
	for _, v := range m.mapTaskStates {
		if v == IDLE || v == BUSY {
			mapsAllCompleted = false
			break
		}
	}

	if mapsAllCompleted {
		m.mapCompleted = true
	}

	reducesAllCompleted := true
	for _, v := range m.reduceTaskStates {
		if v == IDLE || v == BUSY {
			reducesAllCompleted = false
			break
		}
	}

	if reducesAllCompleted {
		m.reduceCompleted = true
	}

	m.mu.Unlock()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// check if all map tasks are completed
	m.mu.Lock()
	if m.mapCompleted && m.reduceCompleted {
		// remove the intermediate files
		for _, v := range m.intermediateFiles {
			for _, f := range v {
				os.Remove(f)
			}
		}

		return true
	}
	m.mu.Unlock()
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		inputFiles:        files,
		mapTaskStates:     make([]int, len(files)),
		reduceTaskStates:  make([]int, nReduce),
		intermediateFiles: make([][]string, nReduce),
		outputFiles:       make([]string, nReduce),
		mapTaskTimers:     make([]int, len(files)),
		reduceTaskTimers:  make([]int, nReduce),
		mapCompleted:      false,
		reduceCompleted:   false,
		mu:                &sync.Mutex{},
	}

	maxBusyTimer := 10

	go func() {
		for {
			time.Sleep(time.Second)

			m.mu.Lock()
			for i, v := range m.mapTaskStates {
				if v == BUSY {
					m.mapTaskTimers[i]++
					if m.mapTaskTimers[i] > maxBusyTimer {
						fmt.Println("Map task timed out: ", i)
						m.mapTaskStates[i] = IDLE
						m.mapTaskTimers[i] = 0
					}
				}
			}

			for i, v := range m.reduceTaskStates {
				if v == BUSY {
					m.reduceTaskTimers[i]++
					if m.reduceTaskTimers[i] > 10 {
						fmt.Println("Reduce task timed out: ", i)
						m.reduceTaskStates[i] = IDLE
						m.reduceTaskTimers[i] = 0
					}
				}
			}
			m.mu.Unlock()
		}
	}()

	fmt.Println("MakeMaster: ", m)

	m.server()
	return &m
}
