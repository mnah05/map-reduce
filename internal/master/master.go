package master

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type MapTask struct {
	ID       int
	Filename string
	Status   TaskStatus
}

type Master struct {
	mu                sync.Mutex
	MapTask           []MapTask
	mapDone           chan struct{}
	intermediateFiles []string
	reduceDone        bool
	nReduce           int
}

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
	log.Printf("Master loaded %d input files from %s", len(tasks), inputDir)
	return &Master{
		MapTask: tasks,
		mapDone: make(chan struct{}),
		nReduce: 1,
	}
}

func (m *Master) GetMapTask(req *mrpc.GetMapTaskRequest, resp *mrpc.GetMapTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, task := range m.MapTask {
		if task.Status == Idle {
			m.MapTask[i].Status = InProgress
			resp.Filename = task.Filename
			resp.TaskID = task.ID
			log.Printf("Master: assigned map task %d (%s)", task.ID, task.Filename)
			return nil
		}
	}
	log.Println("Master: no idle map tasks available")
	return nil
}

func (m *Master) MapDone(req *mrpc.MapDoneRequest, resp *mrpc.MapDoneResponse) error {
	m.mu.Lock()
	var taskID int
	found := false
	for i, task := range m.MapTask {
		if task.Status == InProgress {
			m.MapTask[i].Status = Done
			taskID = task.ID
			found = true
			break
		}
	}
	if found {
		m.intermediateFiles = append(m.intermediateFiles, req.IntermediateFiles...)
		log.Printf("Master: map task %d done, intermediate files: %v", taskID, req.IntermediateFiles)
	} else {
		log.Println("Master: MapDone called but no in-progress task found")
		m.mu.Unlock()
		return nil
	}
	allDone := true
	for _, task := range m.MapTask {
		if task.Status != Done {
			allDone = false
			break
		}
	}
	m.mu.Unlock()
	if allDone {
		close(m.mapDone)
		log.Println("Master: all map tasks complete")
	}
	return nil
}

func (m *Master) GetReduceTask(req *mrpc.GetReduceTaskRequest, resp *mrpc.GetReduceTaskResponse) error {
	<-m.mapDone
	m.mu.Lock()
	resp.IntermediateFiles = m.intermediateFiles
	m.mu.Unlock()
	log.Println("Master: reduce task dispatched")
	return nil
}

func (m *Master) ReduceDone(req *mrpc.ReduceDoneRequest, resp *mrpc.ReduceDoneResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reduceDone = true
	log.Println("Master: reduce phase complete. Job done.")
	return nil
}

func (m *Master) Start() {
	rpc.Register(m)
	l, err := net.Listen("tcp", mrpc.MasterAddress)
	if err != nil {
		log.Fatal("Master failed to listen:", err)
	}
	log.Println("Master listening on", mrpc.MasterAddress)
	go rpc.Accept(l)
}
