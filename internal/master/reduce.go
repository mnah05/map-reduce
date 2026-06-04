package master

import (
	"log"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

// GetReduceTask is an RPC handler called by Workers to claim an idle reduce bucket.
// It blocks until all map tasks have completed, then assigns the next idle reduce bucket.
func (m *Master) GetReduceTask(req *mrpc.Empty, resp *mrpc.GetReduceTaskResponse) error {
	// Wait until all map tasks are done before allowing any reduce task
	<-m.mapDone
	m.mu.Lock()
	defer m.mu.Unlock()
	resp.NReduce = m.nReduce
	for i, status := range m.reduceTasks {
		if status == Idle {
			m.reduceTasks[i] = InProgress
			resp.Bucket = i
			log.Printf("Master: assigned reduce bucket %d", i)
			return nil
		}
	}
	log.Println("Master: no idle reduce tasks available")
	resp.Bucket = -1
	return nil
}

// ReduceDone is an RPC handler called by Workers to mark a reduce bucket as completed.
// When all reduce buckets are done, it logs job completion.
func (m *Master) ReduceDone(req *mrpc.ReduceDoneRequest, resp *mrpc.Empty) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.Bucket < 0 || req.Bucket >= len(m.reduceTasks) {
		log.Printf("Master: ReduceDone called with invalid bucket %d", req.Bucket)
		return nil
	}
	m.reduceTasks[req.Bucket] = Done
	log.Printf("Master: reduce bucket %d done", req.Bucket)
	allDone := true
	for _, status := range m.reduceTasks {
		if status != Done {
			allDone = false
			break
		}
	}
	if allDone {
		log.Println("Master: all reduce tasks complete. Job done.")
	}
	return nil
}
