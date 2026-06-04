// Package worker implements the MapReduce Worker node.
// Workers pull map tasks from the Master, process them, then proceed
// to pull and process reduce tasks.
package worker

import (
	"log"
	"net/rpc"
	"os"
	"sync"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

// RunWorker connects to the Master and runs the map phase (multiple concurrent workers)
// followed by the reduce phase. It blocks until all phases are complete.
func RunWorker() {
	client, err := rpc.Dial("tcp", mrpc.MasterAddress)
	if err != nil {
		log.Fatalf("Worker failed to connect to master: %v", err)
	}
	defer client.Close()

	if err := os.MkdirAll("mr-out", 0755); err != nil {
		log.Fatalf("Worker failed to create mr-out directory: %v", err)
	}

	// --- Map phase: spawn several goroutines to process map tasks concurrently ---
	var wg sync.WaitGroup
	numWorkers := 3
	for range numWorkers {
		wg.Add(1)
		go processMapTasks(client, &wg)
	}
	wg.Wait()
	log.Println("Map phase complete, starting reduce phase")

	// --- Reduce phase: spawn several goroutines to claim and process reduce buckets ---
	numReducers := 3
	var reduceWg sync.WaitGroup
	for range numReducers {
		reduceWg.Add(1)
		go func() {
			defer reduceWg.Done()
			processReduceTask(client)
		}()
	}
	reduceWg.Wait()
	log.Println("Worker: all done")
}
