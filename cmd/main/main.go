// Command main starts the MapReduce Master node.
// It loads input files from the "input" directory and begins
// listening for RPC connections from Workers on localhost:1234.
package main

import "github.com/mnah05/map-reduce/internal/master"

func main() {
	m := master.NewMaster("input")
	m.Start()
	select {} // keep master alive indefinitely
}
