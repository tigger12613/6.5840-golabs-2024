package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// we have three bockets for tasks, ToDoTasks stores task todo, DoingTasks stores task ongoing, DoneTasks store complete tasks
// task bockets with no reduce prefix is for map.
// TasksLock locks all of the task bockets.
// The hole proccess is at worker.go -> Worker.
type Coordinator struct {
	// Your definitions here.
	ToDoTasks        map[int]Task
	DoingTasks       map[int]Task
	DoneTasks        map[int]Task
	ToDoReduceTasks  map[int]ReduceTask
	DoingReduceTasks map[int]ReduceTask
	DoneReduceTasks  map[int]ReduceTask
	TasksLock        sync.Mutex
	MrReduceNumber   int
	TaskCount        int
	TaskCountLock    sync.Mutex
	SerialNum        int
	SerialNumLock    sync.Mutex
	quit             chan struct{}
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
func (c *Coordinator) GetTask(args *ExampleArgs, task *Task) error {
	c.TasksLock.Lock()
	task.TaskType = NoTask
	for serial, t := range c.ToDoTasks {
		*task = t
		c.DoingTasks[serial] = t
		delete(c.ToDoTasks, serial)
		break
	}
	c.TasksLock.Unlock()
	return nil
}
func (c *Coordinator) GetReduceTask(args *ExampleArgs, task *ReduceTask) error {
	c.TasksLock.Lock()
	task.TaskType = NoTask
	for serial, t := range c.ToDoReduceTasks {
		*task = t
		c.DoingReduceTasks[serial] = t
		delete(c.ToDoReduceTasks, serial)
		break
	}
	c.TasksLock.Unlock()
	return nil
}

func (c *Coordinator) IsMapFinished(args *ExampleArgs, isMapFinishedReply *IsFinishedReply) error {
	c.TasksLock.Lock()
	if len(c.ToDoTasks)+len(c.DoingTasks) > 0 {
		isMapFinishedReply.IsFinished = false
	} else {
		isMapFinishedReply.IsFinished = true
	}
	c.TasksLock.Unlock()
	return nil
}
func (c *Coordinator) IsReduceFinished(args *ExampleArgs, isMapFinishedReply *IsFinishedReply) error {
	c.TasksLock.Lock()
	c.TaskCountLock.Lock()
	if c.TaskCount == len(c.DoneReduceTasks) && c.TaskCount > 0 {
		isMapFinishedReply.IsFinished = true
	} else {
		isMapFinishedReply.IsFinished = false
	}
	c.TaskCountLock.Unlock()
	c.TasksLock.Unlock()
	return nil
}
func (c *Coordinator) WorkFinish(args *Task, reply *ExampleReply) error {
	c.TasksLock.Lock()
	delete(c.DoingTasks, args.SerialNum)
	c.DoneTasks[args.SerialNum] = *args
	if len(c.DoingTasks) == 0 && len(c.ToDoTasks) == 0 {
		c.TasksLock.Unlock()
		c.startReduce()
		c.TasksLock.Lock()
	}
	c.TasksLock.Unlock()
	return nil
}
func (c *Coordinator) ReduceFinish(args *ReduceTask, reply *ExampleReply) error {
	c.TasksLock.Lock()
	delete(c.DoingReduceTasks, args.SerialNum)
	c.DoneReduceTasks[args.SerialNum] = *args
	c.TasksLock.Unlock()
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
	go c.checkDoingToLong()
}

func (c *Coordinator) appendTasks(filenames []string) {
	c.TasksLock.Lock()
	for _, fileName := range filenames {
		serial := c.getNewSerialNum()
		c.ToDoTasks[serial] = Task{fileName, []string{}, HaveTask, serial, c.MrReduceNumber}
	}
	c.TasksLock.Unlock()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.TaskCountLock.Lock()
	c.TasksLock.Lock()
	ret := false
	if c.TaskCount == len(c.DoneReduceTasks) && c.TaskCount > 0 {
		ret = true
	} else {
		ret = false
	}
	c.TasksLock.Unlock()
	c.TaskCountLock.Unlock()

	// Your code here.
	if ret {
		close(c.quit)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MrReduceNumber = nReduce
	c.ToDoTasks = make(map[int]Task)
	c.DoingTasks = make(map[int]Task)
	c.DoneTasks = make(map[int]Task)
	c.ToDoReduceTasks = map[int]ReduceTask{}
	c.DoingReduceTasks = map[int]ReduceTask{}
	c.DoneReduceTasks = map[int]ReduceTask{}
	c.appendTasks(files)
	c.quit = make(chan struct{})
	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) checkDoingToLong() {
	ticker := time.NewTicker(5 * time.Second)
	lastTime := make(map[int]int)
	lastTimeReduce := make(map[int]int)
	for {
		select {
		case <-ticker.C:
			c.TasksLock.Lock()
			// bs, _ := json.Marshal(c.ToDoTasks)
			// log.Println("todo:", string(bs))
			// dt, _ := json.Marshal(c.DoingTasks)
			// log.Println("doing:", string(dt))
			// rtd, _ := json.Marshal(c.ToDoReduceTasks)
			// log.Println("reduce todo:", string(rtd))
			// rdt, _ := json.Marshal(c.DoingReduceTasks)
			// log.Println("reduce doing:", string(rdt))
			// log.Println("reduce todo:", len(c.ToDoReduceTasks))
			// log.Println("reduce doing:", len(c.DoingReduceTasks))
			// log.Println("reduce done:", len(c.DoneReduceTasks))
			for serial, task := range c.DoingTasks {
				if lastTime[serial] == 1 {
					c.ToDoTasks[serial] = task
					delete(c.DoingTasks, serial)
					delete(lastTime, serial)
				} else {
					lastTime[serial] = 1
				}
			}
			for serial, task := range c.DoingReduceTasks {
				if lastTimeReduce[serial] == 1 {
					c.ToDoReduceTasks[serial] = task
					delete(c.DoingReduceTasks, serial)
					delete(lastTimeReduce, serial)
				} else {
					lastTimeReduce[serial] = 1
				}
			}
			c.TasksLock.Unlock()
		case <-c.quit:
			ticker.Stop()
			return
		}
	}
}

func (c *Coordinator) getNewSerialNum() int {
	var res int
	c.SerialNumLock.Lock()
	res = c.SerialNum
	c.SerialNum += 1
	c.SerialNumLock.Unlock()
	return res
}
func (c *Coordinator) startReduce() {
	c.TasksLock.Lock()
	c.TaskCountLock.Lock()
	inputfiles := make([]string, 0)
	for _, v := range c.DoneTasks {
		inputfiles = append(inputfiles, v.OutputFile...)
	}
	for index := 0; index < c.MrReduceNumber; index++ {
		c.ToDoReduceTasks[index] = ReduceTask{inputfiles, index, HaveTask, c.MrReduceNumber}
		c.TaskCount += 1
	}
	c.TaskCountLock.Unlock()
	c.TasksLock.Unlock()

}

// func (c *Coordinator) readIntermediate() []KeyValue {
// 	kva := make([]KeyValue, 0)
// 	for _, task := range c.DoneTasks {
// 		file, err := os.Open(task.OutputFile)
// 		if err != nil {
// 			log.Fatalf("read intermediate fail: %s\n", task.OutputFile)
// 		}
// 		dec := json.NewDecoder(file)
// 		for {
// 			var kv KeyValue
// 			if err := dec.Decode(&kv); err != nil {
// 				break
// 			}
// 			kva = append(kva, kv)
// 		}
// 	}
// 	return kva
// }
