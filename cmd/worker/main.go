// Command worker starts a MapReduce Worker node.
// It connects to the Master at localhost:1234 and begins
// pulling map tasks, then reduce tasks, until the job is done.
package main

import "github.com/mnah05/map-reduce/internal/worker"

func main() {
	worker.RunWorker()
}
