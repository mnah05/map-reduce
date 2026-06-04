package master

import (
	"log"
	"time"

	mrpc "github.com/mnah05/map-reduce/internal/rpc"
)

// GetMapTask is an RPC handler called by Workers to claim an idle map task.
// It assigns the next idle task, marks it InProgress, and starts a timeout goroutine.
func (m *Master) GetMapTask(req *mrpc.Empty, resp *mrpc.GetMapTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	resp.NReduce = m.nReduce
	for i, task := range m.MapTask {
		if task.Status == Idle {
			m.MapTask[i].Status = InProgress
			resp.Filename = task.Filename
			resp.TaskID = task.ID
			log.Printf("Master: assigned map task %d (%s)", task.ID, task.Filename)
			// Start a timeout goroutine: if the task isn't reported done in time, reset it to Idle
			go func(taskID int) {
				time.Sleep(mapTaskTimeout)
				m.mu.Lock()
				if taskID < len(m.MapTask) && m.MapTask[taskID].Status == InProgress {
					m.MapTask[taskID].Status = Idle
					log.Printf("Master: map task %d timed out, reset to Idle", taskID)
				}
				m.mu.Unlock()
			}(task.ID)
			return nil
		}
	}
	log.Println("Master: no idle map tasks available")
	return nil
}

// MapDone is an RPC handler called by Workers to mark a map task as completed.
// Once all map tasks are done, it closes the mapDone channel to unblock the reduce phase.
func (m *Master) MapDone(req *mrpc.MapDoneRequest, resp *mrpc.Empty) error {
	m.mu.Lock()
	if req.TaskID < 0 || req.TaskID >= len(m.MapTask) {
		log.Printf("Master: MapDone called with invalid task ID %d", req.TaskID)
		m.mu.Unlock()
		return nil
	}
	if m.MapTask[req.TaskID].Status != InProgress {
		log.Printf("Master: MapDone called for task %d but status is %v (may have timed out or been already done)", req.TaskID, m.MapTask[req.TaskID].Status)
		m.mu.Unlock()
		return nil
	}
	m.MapTask[req.TaskID].Status = Done
	m.intermediateFiles = append(m.intermediateFiles, req.IntermediateFiles...)
	log.Printf("Master: map task %d done, intermediate files: %v", req.TaskID, req.IntermediateFiles)
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
