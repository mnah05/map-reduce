package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

func reduceFunc(key string, values []string) string {
	total := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		total += n
	}
	return strconv.Itoa(total)
}

func RunReduceWorker() {
	client, err := rpc.Dial("tcp", mrpc.MasterAddress)
	if err != nil {
		log.Fatal("Reduce worker failed to connect to master:", err)
	}
	defer client.Close()

	req := mrpc.GetReduceTaskRequest{}
	resp := mrpc.GetReduceTaskResponse{}
	for {
		if err := client.Call("Master.GetReduceTask", &req, &resp); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	log.Println("Reduce worker got task, reading files:", resp.IntermediateFiles)

	var kva []KV
	for _, fname := range resp.IntermediateFiles {
		f, err := os.Open(fname)
		if err != nil {
			log.Fatalf("Reduce worker failed to open intermediate file %s: %v", fname, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KV
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	if err := os.MkdirAll("mr-out", 0755); err != nil {
		log.Fatalf("Reduce worker failed to create mr-out directory: %v", err)
	}

	outFile, err := os.Create("mr-out/mr-out-0")
	if err != nil {
		log.Fatalf("Reduce worker failed to create output file: %v", err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Val)
		}
		output := reduceFunc(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()

	doneReq := mrpc.ReduceDoneRequest{}
	doneResp := mrpc.ReduceDoneResponse{}
	for {
		if err := client.Call("Master.ReduceDone", &doneReq, &doneResp); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	log.Println("Reduce worker done")
}
