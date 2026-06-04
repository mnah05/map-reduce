// Package rpc defines shared RPC types used for communication between the Master and Workers.
package rpc

// MasterAddress is the TCP address the Master listens on and Workers connect to.
const (
	MasterAddress = "localhost:1234"
)

// Empty is a placeholder for RPC calls that need no request/response data.
type Empty struct{}

// GetMapTaskResponse is sent from Master to Worker when assigning a map task.
type GetMapTaskResponse struct {
	Filename string // path to the input file to process
	TaskID   int    // unique identifier for this map task
	NReduce  int    // number of reduce buckets (determines intermediate file count)
}

// MapDoneRequest is sent from Worker to Master after a map task completes.
type MapDoneRequest struct {
	TaskID            int      // which map task finished
	IntermediateFiles []string // list of intermediate file paths produced
}

// GetReduceTaskResponse is sent from Master to Worker when assigning a reduce task.
type GetReduceTaskResponse struct {
	Bucket  int // which reduce bucket to process (-1 if none available)
	NReduce int // total number of reduce buckets
}

// ReduceDoneRequest is sent from Worker to Master after a reduce task completes.
type ReduceDoneRequest struct {
	Bucket int // which reduce bucket finished
}
