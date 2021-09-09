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
	"strconv"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// args := AssignArgs{}
	// reply := AssignReply{}

	for {
		args := AssignArgs{}
		reply := AssignReply{}
		if !call("Coordinator.AssignWork", &args, &reply) {
			log.Fatalf("coordinator exit && worker exit")
			break
		}

		fmt.Println(reply)
		if reply.Kind == "map" {
			filenames := reply.MapInputFiles
			intermediates := [][]KeyValue{}
			for i := 0; i < reply.NReduce; i++ {
				intermediates = append(intermediates, []KeyValue{})
			}

			for _, filename := range filenames {
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
				for _, kv := range kva {
					intermediates[ihash(kv.Key)%reply.NReduce] = append(intermediates[ihash(kv.Key)%reply.NReduce], kv)
				}
			}

			mapoutputfiles := []string{}
	
			for index, intermediate := range intermediates {
				oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
				ofile, _ := os.Create(oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range intermediate {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", kv)
					}
				}
				mapoutputfiles = append(mapoutputfiles, oname)
			}

			completeArgs := CompleteArgs{
				WorkID: reply.WorkID,
				MapOutputFiles: mapoutputfiles,
			}
			CompleteReply := CompleteReply{}
			if !call("Coordinator.CompleteWork", &completeArgs, &CompleteReply) {
				index := 0
				for index < len(intermediates) {
					oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
					err := os.Remove(oname)
					if err != nil {
						log.Fatalf("cannot remove %v", oname)
					}
					index++
				}
				log.Fatalf("coordinator exit && woker exit")
				return
			}
			index := 0
			for index < len(intermediates){
				oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
				err := os.Remove(oname)
				if err != nil {
					log.Fatalf("cannot remove %v", oname)
				}
				index++
			}
		} else if reply.Kind == "reduce" {
			filenames := reply.ReduceInputFiles
			fmt.Println(filenames)
			fmt.Println(reply)
			intermediate := []KeyValue{}
			for _, filename := range filenames {
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

			reduceoutputfiles := []string{}
		
			oname := reply.WorkID + ":mr-out-" + strconv.Itoa(reply.Index) 
			fmt.Println(oname) //
			ofile, _ := os.Create(oname)

			reduceoutputfiles = append(reduceoutputfiles, oname)

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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		
				i = j
			}
		
			ofile.Close()
	
			completeArgs := CompleteArgs{
				WorkID: reply.WorkID,
				ReduceOutputFiles: reduceoutputfiles,
			}
			CompleteReply := CompleteReply{}
			if !call("Coordinator.CompleteWork", &completeArgs, &CompleteReply) {
				err := os.Remove(oname)
				if err != nil {
					log.Fatalf("cannot remove %v", oname)
				}
				log.Fatalf("coordinator exit && woker exit")
				return
			}
			err := os.Remove(oname)
			if err != nil {
				log.Fatalf("cannot remove %v", oname)
			}
		} else {
			log.Fatalf("worker expected exit")
			return
		}

	}

	// for call("Coordinator.AssignWork", &args, &reply) {
	// 	fmt.Println(reply)
	// 	if reply.Kind == "map" {
	// 		filenames := reply.MapInputFiles
	// 		intermediates := [][]KeyValue{}
	// 		for i := 0; i < reply.NReduce; i++ {
	// 			intermediates = append(intermediates, []KeyValue{})
	// 		}

	// 		for _, filename := range filenames {
	// 			file, err := os.Open(filename)
	// 			if err != nil {
	// 				log.Fatalf("cannot open %v", filename)
	// 			}
	// 			content, err := ioutil.ReadAll(file)
	// 			if err != nil {
	// 				log.Fatalf("cannot read %v", filename)
	// 			}
	// 			file.Close()
	// 			kva := mapf(filename, string(content))
	// 			for _, kv := range kva {
	// 				intermediates[ihash(kv.Key)%reply.NReduce] = append(intermediates[ihash(kv.Key)%reply.NReduce], kv)
	// 			}
	// 		}

	// 		mapoutputfiles := []string{}
	
	// 		for index, intermediate := range intermediates {
	// 			oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
	// 			ofile, _ := os.Create(oname)
	// 			enc := json.NewEncoder(ofile)
	// 			for _, kv := range intermediate {
	// 				err := enc.Encode(&kv)
	// 				if err != nil {
	// 					log.Fatalf("cannot encode %v", kv)
	// 				}
	// 			}
	// 			mapoutputfiles = append(mapoutputfiles, oname)
	// 		}

	// 		completeArgs := CompleteArgs{
	// 			WorkID: reply.WorkID,
	// 			MapOutputFiles: mapoutputfiles,
	// 		}
	// 		CompleteReply := CompleteReply{}
	// 		if !call("Coordinator.CompleteWork", &completeArgs, &CompleteReply) {
	// 			index := 0
	// 			for index < len(intermediates) {
	// 				oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
	// 				err := os.Remove(oname)
	// 				if err != nil {
	// 					log.Fatalf("cannot remove %v", oname)
	// 				}
	// 				index++
	// 			}
	// 			log.Fatalf("coordinator exit && woker exit")
	// 			return
	// 		}
	// 		index := 0
	// 		for index < len(intermediates){
	// 			oname := reply.WorkID + ":mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(index)
	// 			err := os.Remove(oname)
	// 			if err != nil {
	// 				log.Fatalf("cannot remove %v", oname)
	// 			}
	// 			index++
	// 		}
	// 	} else if reply.Kind == "reduce" {
	// 		filenames := reply.ReduceInputFiles
	// 		fmt.Println(filenames)
	// 		fmt.Println(reply)
	// 		intermediate := []KeyValue{}
	// 		for _, filename := range filenames {
	// 			file, err := os.Open(filename)
	// 			if err != nil {
	// 				log.Fatalf("cannot open %v", filename)
	// 			}
	// 			dec := json.NewDecoder(file)
	// 			for {
	// 				var kv KeyValue
	// 				if err := dec.Decode(&kv); err != nil {
	// 					break
	// 				}
	// 				intermediate = append(intermediate, kv)
	// 			}
	// 		}
		
	// 		sort.Sort(ByKey(intermediate))

	// 		reduceoutputfiles := []string{}
		
	// 		oname := reply.WorkID + ":mr-out-" + strconv.Itoa(reply.Index) 
	// 		fmt.Println(oname) //
	// 		ofile, _ := os.Create(oname)

	// 		reduceoutputfiles = append(reduceoutputfiles, oname)

	// 		i := 0
	// 		for i < len(intermediate) {
	// 			j := i + 1
	// 			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// 				j++
	// 			}
	// 			values := []string{}
	// 			for k := i; k < j; k++ {
	// 				values = append(values, intermediate[k].Value)
	// 			}
	// 			output := reducef(intermediate[i].Key, values)
	// 			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		
	// 			i = j
	// 		}
		
	// 		ofile.Close()
	
	// 		completeArgs := CompleteArgs{
	// 			WorkID: reply.WorkID,
	// 			ReduceOutputFiles: reduceoutputfiles,
	// 		}
	// 		CompleteReply := CompleteReply{}
	// 		if !call("Coordinator.CompleteWork", &completeArgs, &CompleteReply) {
	// 			err := os.Remove(oname)
	// 			if err != nil {
	// 				log.Fatalf("cannot remove %v", oname)
	// 			}
	// 			log.Fatalf("coordinator exit && woker exit")
	// 			return
	// 		}
	// 		err := os.Remove(oname)
	// 		if err != nil {
	// 			log.Fatalf("cannot remove %v", oname)
	// 		}
	// 	} else {
	// 		log.Fatalf("worker expected exit")
	// 		return
	// 	}
	// }
	// log.Fatalf("coordinator exit && worker exit")
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
