.PHONY: build run clean

build:
	go build -o bin/master cmd/main/main.go
	go build -o bin/worker cmd/worker/main.go

run: build
	@echo "=== Starting MapReduce ==="
	-kill -9 $$(lsof -ti:1234) 2>/dev/null
	rm -rf mr-out
	./bin/master &
	MASTER_PID=$$!; \
	sleep 1; \
	./bin/worker & \
	./bin/worker & \
	./bin/worker & \
	wait; \
	kill $$MASTER_PID 2>/dev/null || true
	@echo "=== Done! Output in mr-out/ ==="

clean:
	rm -rf bin mr-out
