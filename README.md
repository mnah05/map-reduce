# map-reduce

A MapReduce implementation in Go, based on MIT 6.824/6.5840.

## Usage

```sh
make run
```

Builds the master, map, and reduce binaries, then runs the pipeline on the input files in `input/`. Output is written to `mr-out/`.

## Project Structure

- `cmd/main/` — master binary entrypoint
- `cmd/map/` — map worker binary
- `cmd/reduce/` — reduce worker binary
- `internal/master/` — master coordination logic
- `internal/worker/` — worker (map/reduce) logic
- `internal/rpc/` — RPC definitions between master and workers
- `input/` — sample input files
