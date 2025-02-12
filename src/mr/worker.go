package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import (
	"io/ioutil"
	"os"
	"time"
	"strconv"
	"encoding/json"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func FileNameXY (X int, Y int) string {
	s := "./mr"
	s += strconv.Itoa(X)
	s += "-"
	s += strconv.Itoa(Y)
	return s
}
func ApplyMap (mapf func (string, string) [] KeyValue, reply Reply) {
	// create the kva
	file, _ := os.Open(reply.Filename)
	content, _ := ioutil.ReadAll (file)
	file.Close()
	kva := mapf(reply.Filename, string(content))
	
	// create the X-Y files
	encs := []*json.Encoder{}
	for i := 0 ; i < reply.R ; i ++ {
		oname := FileNameXY(reply.TNum,i)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		encs = append(encs,enc)
	}
	// fill the X-Y files
	for _, kv := range kva {
		h := ihash(kv.Key) % reply.R
		encs [h].Encode(&kv)
	}
	FinishedTask("map",reply.TNum)
}
func ApplyReduce (reducef func (string, []string ) string , reply Reply) {
	intermediate := []KeyValue{}
	for i:= 0 ; i < reply.M ; i ++ {
		filename := FileNameXY(i,reply.TNum)
		file, _ := os.Open (filename)
		defer file.Close()
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
	oname := "mr-out-"
	oname += strconv.Itoa(reply.TNum)
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	ofile.Close()
	FinishedTask("reduce",reply.TNum)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := RequestTask()
		if reply.JobType == "map" {
			ApplyMap(mapf,reply)
		} else if reply.JobType == "reduce" {
			ApplyReduce(reducef,reply)
		} else if reply.JobType == "wait" {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

func RequestTask () Reply {
	args := Args {}
	reply := Reply{}
	ok := call ("Coordinator.Task", &args, &reply);
	if !ok {
		fmt.Printf ("call failed!\n");
	}
	return reply
}
func FinishedTask (JobType string, TNum int) Reply {
	args := Args {
		JobType: JobType,
		TNum: TNum,
	}
	reply := Reply{}
	ok := call ("Coordinator.Upd", &args, &reply);
	if !ok {
		fmt.Printf ("call failed!\n");
	}

	return reply
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

	fmt.Println(err)
	return false
}
