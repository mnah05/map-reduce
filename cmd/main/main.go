package main

import "github.com/mnah05/map-reduce/internal/master"

func main() {
	m := master.NewMaster("input")
	m.Start()
	select {} // keep master alive
}
