package worker

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

// KV represents a key-value pair emitted by the map phase and consumed by the reduce phase.
type KV struct {
	Key string
	Val string
}

// mapFunc splits file contents into words and emits a KV ("word", "1") for each word.
func mapFunc(filename string, contents string) []KV {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	var kv []KV
	for _, w := range words {
		kv = append(kv, KV{w, "1"})
	}
	return kv
}

// ihash returns a non-negative hash value used to determine which reduce bucket a key belongs to.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// processMapTasks continuously pulls map tasks from the Master via RPC,
// executes the map function, writes intermediate files (one per reduce bucket),
// and reports completion back to the Master.
func processMapTasks(client *rpc.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		req := mrpc.Empty{}
		resp := mrpc.GetMapTaskResponse{}
		for {
			if err := client.Call("Master.GetMapTask", &req, &resp); err == nil {
				break
			}
			time.Sleep(time.Second)
		}

		if resp.Filename == "" {
			break
		}
		log.Printf("Map worker got task: file=%s taskID=%d nReduce=%d\n", resp.Filename, resp.TaskID, resp.NReduce)

		contents, err := os.ReadFile(resp.Filename)
		if err != nil {
			log.Fatalf("Map worker failed to read input file %s: %v", resp.Filename, err)
		}

		kva := mapFunc(resp.Filename, string(contents))

		files := make([]*os.File, resp.NReduce)
		encoders := make([]*json.Encoder, resp.NReduce)
		for i := range resp.NReduce {
			fname := fmt.Sprintf("mr-out/mr-%d-%d", resp.TaskID, i)
			files[i], err = os.Create(fname)
			if err != nil {
				log.Fatalf("Map worker failed to create intermediate file %s: %v", fname, err)
			}
			encoders[i] = json.NewEncoder(files[i])
		}

		for _, kv := range kva {
			bucket := ihash(kv.Key) % resp.NReduce
			if err := encoders[bucket].Encode(kv); err != nil {
				log.Fatalf("Map worker failed to write intermediate record: %v", err)
			}
		}
		var intermediateFiles []string
		for i, f := range files {
			if err := f.Close(); err != nil {
				log.Printf("Map worker warning: failed to close file: %v", err)
			}
			intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-out/mr-%d-%d", resp.TaskID, i))
		}

		doneReq := mrpc.MapDoneRequest{TaskID: resp.TaskID, IntermediateFiles: intermediateFiles}
		doneResp := mrpc.Empty{}
		for {
			if err := client.Call("Master.MapDone", &doneReq, &doneResp); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		log.Printf("Map worker: task %d done, reported to master", resp.TaskID)
	}
}
