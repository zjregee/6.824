package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type MapTask struct {
	ID         int
	InputFile  string
	IfAssigned bool
	IfFinished bool
	NReduce    int
	OutputFile []string
	StartTime  int64
}

type ReduceTask struct {
	ID int
	InputFiles []string
	IfAssigned bool
	IfFinished bool
	OutputFile string
	StartTime  int64
}

type Coordinator struct {
	MapTasks              []MapTask
	ReduceTasks           []ReduceTask
	HaveAssignMapTasks    int
	HaveAssignReduceTasks int
	ReduceFlag            bool
	MapFlag               bool
	WorkID                int
}

var Workers map[string]Work

type Work struct {
	Kind  string
	Index int
}

var mutex sync.Mutex

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

func (c *Coordinator) AssignWork(agrs *AssignArgs, reply *AssignReply) error {
	mutex.Lock()
	// 分配Map任务
	mapTaskCount := len(c.MapTasks)
	index := 0
	ok := true
	for c.HaveAssignMapTasks == mapTaskCount && !c.MapFlag {
		ok, index = c.CheckMapWork()
		if !ok {
			break
		}
		time.Sleep(time.Second)
	}

	if !c.MapFlag {
		// Map任务分配完但有一个超时直接分配这个
		if !ok {
			reply.Index = index
			reply.Kind = "map"
			reply.MapInputFiles = append([]string{}, c.MapTasks[index].InputFile)
			reply.NReduce = c.MapTasks[index].NReduce
			reply.WorkID = strconv.Itoa(c.WorkID)
			c.WorkID++

			c.MapTasks[index].StartTime = time.Now().Unix()
			
			work := Work{
				Kind: "map",
				Index: reply.Index,
			}
			Workers[reply.WorkID] = work
			mutex.Unlock()
			return nil
		}
		// 分配一个尚未分配的任务
		for _, task := range c.MapTasks {
			if task.IfAssigned {
				continue
			}
			reply.Index = task.ID
			reply.Kind = "map"
			reply.MapInputFiles = append([]string{}, c.MapTasks[task.ID].InputFile)
			reply.NReduce = c.MapTasks[task.ID].NReduce
			reply.WorkID = strconv.Itoa(c.WorkID)
			c.WorkID++

			c.MapTasks[task.ID].IfAssigned = true
			c.HaveAssignMapTasks++
			c.MapTasks[index].StartTime = time.Now().Unix()
			
			work := Work{
				Kind: "map",
				Index: reply.Index,
			}
			Workers[reply.WorkID] = work
			mutex.Unlock()
			return nil
		}
		log.Fatalf("Coordinator Distributed Map ERROR!")
		return nil
	}
	// 分配Reduce任务
	reduceTaskCount := len(c.ReduceTasks)
	index = 0
	ok = true
	for c.HaveAssignReduceTasks == reduceTaskCount && !c.ReduceFlag {
		ok, index = c.CheckReduceWork()
		if !ok {
			break
		}
		time.Sleep(time.Second)
	}

	if !c.ReduceFlag {
		if !ok {
			fmt.Println(index)
			reply.Index = index
			reply.Kind = "reduce"
			reduceInputFiles := []string{}
			for i := 0; i<mapTaskCount; i++ {
				reduceInputFiles = append(reduceInputFiles, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(reply.Index))
			}
			reply.ReduceInputFiles = reduceInputFiles
			reply.WorkID = strconv.Itoa(c.WorkID)
			c.WorkID++

			c.ReduceTasks[index].StartTime = time.Now().Unix()

			work := Work{
				Kind: "reduce",
				Index: reply.Index,
			}
			Workers[reply.WorkID] = work
			mutex.Unlock()
			return nil
		}
		// 	分配一个尚未分配的任务
		for _, task := range c.ReduceTasks {
			if task.IfAssigned {
				continue
			}
			reply.Index = task.ID
			fmt.Println(task)
			reply.Kind = "reduce"
			reduceInputFiles := []string{}
			for i := 0; i<mapTaskCount; i++ {
				reduceInputFiles = append(reduceInputFiles, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(reply.Index))
			}
			reply.ReduceInputFiles = reduceInputFiles
			reply.WorkID = strconv.Itoa(c.WorkID)
			c.WorkID++

			c.ReduceTasks[task.ID].IfAssigned = true
			c.HaveAssignReduceTasks++
			c.ReduceTasks[index].StartTime = time.Now().Unix()

			work := Work{
				Kind: "reduce",
				Index: reply.Index,
			}
			Workers[reply.WorkID] = work
			fmt.Println(reply)
			mutex.Unlock()
			return nil
		}
		log.Fatalf("Coordinator Distributed Reduce Error")
		return nil
	}
	log.Fatalf("All Tasks Completed")
	reply.Kind = "exit"
	mutex.Unlock()
	return nil
}

func (c *Coordinator) CompleteWork(args *CompleteArgs, reply *CompleteReply) error {
	mutex.Lock()
	work := Workers[args.WorkID]
	if work.Kind == "map" {
		if c.MapTasks[work.Index].IfFinished {
			mutex.Unlock()
			return nil
		}
		c.MapTasks[work.Index].IfFinished = true

		// 复制文件
		r, _ := regexp.Compile(":(.*)")
		for _, ofile := range args.MapOutputFiles {
			file := r.FindString(ofile)[1:]
			source, err := os.Open(ofile)
			if err != nil {
				log.Fatalf("cannot open file %v", ofile)
			}
			defer source.Close()

			dest, err := os.Create(file)
			if err != nil {
				log.Fatalf("cannot create file %v", dest)
			}
			defer dest.Close()

			_, err = io.Copy(dest, source)
			if err != nil {
				log.Fatalf("cannot copy file %v %v", dest, ofile)
			}
		}

		flag := true
		for _, task := range c.MapTasks {
			if !task.IfFinished {
				flag = false
				break
			}
		}
		c.MapFlag = flag
		mutex.Unlock()
		return nil
	}

	if work.Kind == "reduce" {
		if c.ReduceTasks[work.Index].IfFinished {
			mutex.Unlock()
			return nil
		}
		c.ReduceTasks[work.Index].IfFinished = true

		// 复制文件
		r, _ := regexp.Compile(":(.*)")
		for _, ofile := range args.ReduceOutputFiles {
			file := r.FindString(ofile)[1:]
			source, err := os.Open(ofile)
			if err != nil {
				log.Fatalf("cannot open file %v", ofile)
			}
			defer source.Close()

			dest, err := os.Create(file)
			if err != nil {
				log.Fatalf("cannot create file %v", dest)
			}
			defer dest.Close()

			_, err = io.Copy(dest, source)
			if err != nil {
				log.Fatalf("cannot copy file %v %v", dest, ofile)
			}
		}

		flag := true
		for _, task := range c.ReduceTasks {
			if !task.IfFinished {
				flag = false
				break
			}
		}

		if flag {
			// 删除map的中间文件
			for i := 0; i < len(c.MapTasks); i++ {
				for j := 0; j < len(c.ReduceTasks); j++ {
					oname := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(j)
					err := os.Remove(oname)
					if err != nil {
						log.Fatalf("cannot remove %v", oname)
					}
				}
			}
		}

		c.ReduceFlag = flag
		mutex.Unlock()
		return nil
	}
	log.Fatalf("Complete Task Error!")
	return nil
}

// 检查是否有Map任务超时
func (c *Coordinator) CheckMapWork() (bool, int) {
	timeNow := time.Now().Unix()
	for index, task := range c.MapTasks {
		if task.StartTime + 10 < timeNow {
			return false, index
		}
	}
	return true, -1
}

// 检查是否有Reduce任务超时
func (c *Coordinator) CheckReduceWork() (bool, int) {
	timeNow := time.Now().Unix()
	for index, task := range c.ReduceTasks {
		if task.StartTime + 10 < timeNow {
			return false, index
		}
	}
	return true, -1
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
	ret := false

	if c.MapFlag && c.ReduceFlag {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks: []MapTask{},
		ReduceTasks: []ReduceTask{},
		HaveAssignMapTasks: 0,
		HaveAssignReduceTasks: 0,
		ReduceFlag: false,
		MapFlag: false,
		WorkID: 0, // 实际工作的WorkID从1开始
	}

	for index, file := range files {
		task := MapTask{
			ID: index,
			InputFile: file,
			IfAssigned: false,
			IfFinished: false,
			NReduce: nReduce,
			OutputFile: []string{},
			StartTime: 0,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	for index := 0; index < nReduce; index++ {
		task := ReduceTask{
			ID: index,
			InputFiles: []string{},
			IfAssigned: false,
			IfFinished: false,
			OutputFile: "",
			StartTime: 0,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	Workers = map[string]Work{}

	c.server()
	return &c
}
