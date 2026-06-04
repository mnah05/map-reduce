package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

// reduceFunc sums all values for a given key and returns the total as a string.
func reduceFunc(values []string) string {
	total := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		total += n
	}
	return strconv.Itoa(total)
}

// processReduceTask claims a reduce bucket from the Master via RPC,
// reads all intermediate files for that bucket, groups by key,
// applies the reduce function, and writes the final output.
func processReduceTask(client *rpc.Client) {
	req := mrpc.Empty{}
	resp := mrpc.GetReduceTaskResponse{}
	for {
		if err := client.Call("Master.GetReduceTask", &req, &resp); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if resp.Bucket < 0 {
		log.Println("Reduce worker: no reduce task assigned")
		return
	}
	log.Printf("Reduce worker got bucket %d / %d\n", resp.Bucket, resp.NReduce)

	// Gather all intermediate files for this bucket (mr-*-<bucket>)
	pattern := fmt.Sprintf("mr-out/mr-*-%d", resp.Bucket)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Reduce worker failed to glob intermediate files: %v", err)
	}
	log.Printf("Reduce worker: reading %d files for bucket %d", len(matches), resp.Bucket)

	var kva []KV
	for _, fname := range matches {
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

	// Sort by key so we can group consecutive records
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	if err := os.MkdirAll("mr-out", 0755); err != nil {
		log.Fatalf("Reduce worker failed to create mr-out directory: %v", err)
	}

	outFile, err := os.Create(fmt.Sprintf("mr-out/mr-out-%d", resp.Bucket))
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
		output := reduceFunc(values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()

	doneReq := mrpc.ReduceDoneRequest{Bucket: resp.Bucket}
	doneResp := mrpc.Empty{}
	for {
		if err := client.Call("Master.ReduceDone", &doneReq, &doneResp); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	log.Printf("Reduce worker: bucket %d done", resp.Bucket)
}
