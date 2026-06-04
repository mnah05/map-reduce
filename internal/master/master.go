// Package master implements the MapReduce Master node.
// It manages map/reduce task assignment, tracks progress, and handles timeouts.
package master

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

const mapTaskTimeout = 10 * time.Second

// TaskStatus represents the lifecycle state of a map or reduce task.
type TaskStatus int

const (
	Idle       TaskStatus = iota // task is waiting to be assigned
	InProgress                   // task is currently assigned to a worker
	Done                         // task has been completed
)

// MapTask holds metadata about a single map task.
type MapTask struct {
	ID       int
	Filename string
	Status   TaskStatus
}

// Master coordinates the entire MapReduce job: assigns tasks to workers,
// tracks completions, and signals phase transitions.
type Master struct {
	mu                sync.Mutex
	MapTask           []MapTask
	mapDone           chan struct{}      // closed when all map tasks finish
	intermediateFiles []string           // collected intermediate file paths
	reduceTasks       []TaskStatus       // status per reduce bucket
	nReduce           int
}

// NewMaster reads input files from inputDir and initializes map/reduce tasks.
func NewMaster(inputDir string) *Master {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Master failed to read input directory %s: %v", inputDir, err)
	}
	var tasks []MapTask
	for i, e := range entries {
		tasks = append(tasks, MapTask{
			ID:       i,
			Filename: filepath.Join(inputDir, e.Name()),
			Status:   Idle,
		})
	}
	nReduce := 3
	reduceTasks := make([]TaskStatus, nReduce)
	log.Printf("Master loaded %d input files from %s, nReduce=%d", len(tasks), inputDir, nReduce)
	return &Master{
		MapTask:     tasks,
		mapDone:     make(chan struct{}),
		reduceTasks: reduceTasks,
		nReduce:     nReduce,
	}
}

// Start registers the Master as an RPC server and begins listening for worker connections.
func (m *Master) Start() {
	rpc.Register(m)
	l, err := net.Listen("tcp", mrpc.MasterAddress)
	if err != nil {
		log.Fatal("Master failed to listen:", err)
	}
	log.Println("Master listening on", mrpc.MasterAddress)
	go rpc.Accept(l)
}
