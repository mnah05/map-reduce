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

type KV struct {
	Key string
	Val string
}

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

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func processMapTasks(client *rpc.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		req := mrpc.GetMapTaskRequest{}
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
		log.Printf("Map worker got task: file=%s taskID=%d\n", resp.Filename, resp.TaskID)

		contents, err := os.ReadFile(resp.Filename)
		if err != nil {
			log.Fatalf("Map worker failed to read input file %s: %v", resp.Filename, err)
		}

		kva := mapFunc(resp.Filename, string(contents))

		nReduce := 1
		files := make([]*os.File, nReduce)
		encoders := make([]*json.Encoder, nReduce)
		for i := range nReduce {
			fname := fmt.Sprintf("mr-out/mr-%d-%d", resp.TaskID, i)
			files[i], err = os.Create(fname)
			if err != nil {
				log.Fatalf("Map worker failed to create intermediate file %s: %v", fname, err)
			}
			encoders[i] = json.NewEncoder(files[i])
		}

		for _, kv := range kva {
			bucket := ihash(kv.Key) % nReduce
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
		doneResp := mrpc.MapDoneResponse{}
		for {
			if err := client.Call("Master.MapDone", &doneReq, &doneResp); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		log.Printf("Map worker: task %d done, reported to master", resp.TaskID)
	}
}

func RunMapWorker() {
	client, err := rpc.Dial("tcp", mrpc.MasterAddress)
	if err != nil {
		log.Fatalf("Map worker failed to connect to master: %v", err)
	}
	defer client.Close()

	if err := os.MkdirAll("mr-out", 0755); err != nil {
		log.Fatalf("Map worker failed to create mr-out directory: %v", err)
	}

	var wg sync.WaitGroup
	numWorkers := 3
	for range numWorkers {
		wg.Add(1)
		go processMapTasks(client, &wg)
	}
	wg.Wait()
	log.Println("Map worker: all done")
}
