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

	// Your worker implementation here.
	for {
		CallGetTask(mapf, reducef)
		time.Sleep(time.Second * 5)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func CallGetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	fmt.Println("CallGetTask:", reply)

	if reply.MapTaskNum != -1 {
		DoMap(mapf, reply.MapTaskNum, reply.Filenames[0], reply.NReduce)
	} else if reply.ReduceTaskNum != -1 {
		DoReduce(reducef, reply.ReduceTaskNum, reply.Filenames)
	}
}

func DoMap(mapf func(string, string) []KeyValue, taskNum int, filename string, nReduce int) {
	fmt.Println("DoMap:", taskNum, filename)

	args := TaskCompletedArgs{
		MapTaskNum:    taskNum,
		ReduceTaskNum: -1,
		status:        0,
	}

	reply := TaskCompletedReply{}

	file, err := os.Open(filename)
	if err != nil {
		TaskCompletedError(filename, &args, &reply)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		TaskCompletedError(filename, &args, &reply)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		reduceBucketNum := ihash(kv.Key) % nReduce
		intermediate[reduceBucketNum] = append(intermediate[reduceBucketNum], kv)
	}

	for i, v := range intermediate {
		filename := fmt.Sprintf("mr-%d-%d", taskNum, i)
		file, err := ioutil.TempFile("./", filename)
		if err != nil {
			TaskCompletedError(filename, &args, &reply)
		}
		enc := json.NewEncoder(file)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				TaskCompletedError(filename, &args, &reply)
			}
		}

		file.Close()
		args.Filenames = append(args.Filenames, file.Name()) // already sorted
	}

	call("Master.TaskCompleted", &args, &reply)
	fmt.Println("CallTaskCompleted:", reply)

	if reply.AlreadyCompleted {
		for _, filename := range args.Filenames {
			os.Remove(filename)
		}
	}
}

func DoReduce(reducef func(string, []string) string, taskNum int, filenames []string) {
	fmt.Println("DoReduce:", taskNum)

	args := TaskCompletedArgs{
		MapTaskNum:    -1,
		ReduceTaskNum: taskNum,
		status:        0,
	}

	reply := TaskCompletedReply{}

	var kva []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			TaskCompletedError(filename, &args, &reply)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	filename := fmt.Sprintf("mr-out-%d", taskNum)
	file, err := ioutil.TempFile("./", filename)
	if err != nil {
		TaskCompletedError(filename, &args, &reply)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}

	file.Close()
	filename = fmt.Sprintf("mr-out-%d", taskNum)
	os.Rename(file.Name(), filename)
	args.Filenames = append(args.Filenames, filename)

	call("Master.TaskCompleted", &args, &reply)
	fmt.Println("CallTaskCompleted:", reply)

	if reply.AlreadyCompleted {
		for _, filename := range args.Filenames {
			os.Remove(filename)
		}
	}
}

func TaskCompletedError(filename string, args *TaskCompletedArgs, reply *TaskCompletedReply) {
	log.Fatalf("Error involving file %v", filename)
	args.status = -1

	call("Master.TaskCompleted", &args, &reply)
	fmt.Println("CallTaskCompleted:", reply)

	if reply.AlreadyCompleted {
		for _, filename := range args.Filenames {
			os.Remove(filename)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
