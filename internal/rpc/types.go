package rpc

const (
	MasterAddress = "localhost:1234"
)

type GetMapTaskRequest struct{}
type GetMapTaskResponse struct {
	Filename string
	TaskID   int
}
type MapDoneRequest struct {
	TaskID            int
	IntermediateFiles []string
}
type MapDoneResponse struct{}

type GetReduceTaskRequest struct{}
type GetReduceTaskResponse struct {
	IntermediateFiles []string
}

// Reduce worker reports completion
type ReduceDoneRequest struct{}
type ReduceDoneResponse struct{}
