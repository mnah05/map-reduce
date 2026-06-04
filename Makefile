.PHONY: build run clean

build:
	go build -o bin/master cmd/main/main.go
	go build -o bin/map cmd/map/map-main.go
	go build -o bin/reduce cmd/reduce/reduce-main.go

run: build
	@echo "=== Starting MapReduce ==="
	-kill -9 $$(lsof -ti:1234) 2>/dev/null
	rm -rf mr-out
	./bin/master &
	MASTER_PID=$$!; \
	sleep 1; \
	./bin/map; \
	sleep 1; \
	./bin/reduce; \
	kill $$MASTER_PID 2>/dev/null || true
	@echo "=== Done! Output in mr-out/mr-out-0 ==="

clean:
	rm -rf bin mr-out
