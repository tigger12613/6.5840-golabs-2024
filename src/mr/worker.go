package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Workers check is Map Finished, if not get a map task and work on.
// Then try to get another map task until Coordinator says Map Finished.
// Then Workers check is Reduce Finished and the process is similar to map.
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	reply := IsFinishedReply{false}
	arg := ExampleArgs{}
	// check IsMapFinished
	ok := call("Coordinator.IsMapFinished", &arg, &reply)
	for ok && !reply.IsFinished {
		// get map task
		task, err := GetTask()
		if err != nil {
			return
		} else if task.TaskType == NoTask {
			// no task mean there is no todo task on Coordinator now, but may have some sometimes.
			ok = call("Coordinator.IsMapFinished", &arg, &reply)
			continue
			//log.Printf("get task:%+v\n", task)
		}
		filename := task.InputFile
		// read files
		file, err := os.Open(filename)
		if err != nil {
			//log.Fatalf("cannot open %v", filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		// do map func
		kva := mapf(task.InputFile, string(content))
		// place the output by hashing the key
		bocket := make([][]KeyValue, 10)
		for i := 0; i < task.NReduce; i++ {
			bocket[i] = []KeyValue{}
		}
		for _, kv := range kva {
			bocket[ihash(kv.Key)%task.NReduce] = append(bocket[ihash(kv.Key)%task.NReduce], kv)
		}
		// save ouput at mr-X-Y, X is input SerialNum, Y is index after hash(bocket[ihash(kv.Key)%task.NReduce]).
		for index, kvs := range bocket {
			outfile := fmt.Sprintf("mr-%v-%v", task.SerialNum, index)
			task.OutputFile = append(task.OutputFile, outfile)
			saveIntermediate(outfile, kvs)
		}

		// done task
		workReply := ExampleReply{}
		done := call("Coordinator.WorkFinish", &task, &workReply)
		for !done {
			done = call("Coordinator.WorkFinish", &task, &workReply)
		}
		time.Sleep(10 * time.Millisecond)
		ok = call("Coordinator.IsMapFinished", &arg, &reply)
	}
	// When Coordinator check all map task is done, Workers now try to get reduce task.
	reply = IsFinishedReply{false}
	ok = call("Coordinator.IsReduceFinished", &arg, &reply)
	for ok && !reply.IsFinished {
		task, err := GetReduceTask()
		if err != nil {
			return
		} else if task.TaskType == NoTask {
			ok = call("Coordinator.IsReduceFinished", &arg, &reply)
			continue
		} else {
			//log.Printf("%d get reduce task:%+v\n", os.Getpid(), task)
		}
		mapValues := map[string][]string{}
		for _, filename := range task.InputFiles {
			if filename[len(filename)-1:] != fmt.Sprintf("%v", task.SerialNum) {
				continue
			}
			file, err := os.Open(filename)
			if err != nil {
				//log.Fatalf("cannot open %v", filename)
				return
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				if mapValues[kv.Key] == nil {
					mapValues[kv.Key] = []string{}
				}
				mapValues[kv.Key] = append(mapValues[kv.Key], kv.Value)

			}
		}
		// first store reduce output on temporary file, because reduce func may crash.
		// after reduce done, rename temp file.
		ofile, err := os.CreateTemp("", "tmp")
		if err != nil {
			log.Fatalf("fail to create new file mr-out-%v", task.SerialNum)
		}
		for key, value := range mapValues {
			output := reducef(key, value)
			fmt.Fprintf(ofile, "%v %v\n", key, output)
		}
		defer os.Remove(ofile.Name())
		os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%v", task.SerialNum))
		// done task
		reduceReply := ExampleReply{}
		res := call("Coordinator.ReduceFinish", &task, &reduceReply)
		for !res {
			res = call("Coordinator.ReduceFinish", &task, &reduceReply)
		}

		time.Sleep(10 * time.Millisecond)
		ok = call("Coordinator.IsReduceFinished", &arg, &reply)
	}
	// uncomment to send the Example RPC to the coordinator.

}

func GetTask() (Task, error) {
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		log.Printf("call failed!\n")
		return Task{}, errors.New("call failed!")
	}

}
func GetReduceTask() (ReduceTask, error) {
	args := ExampleArgs{}
	reply := ReduceTask{}
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		log.Printf("call failed!\n")
		return ReduceTask{}, errors.New("call failed!")
	}

}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
	return false
}

func saveIntermediate(filename string, intermediate []KeyValue) {
	ofile, _ := os.Create(filename)
	defer ofile.Close()
	//encoder := json.NewEncoder(ofile)
	enc := json.NewEncoder(ofile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			break
		}
	}
}
