# Makefile — ConcurrentKV

.PHONY: all build test bench crash clean fmt vet

CXX      ?= g++
GOFLAGS  := CGO_ENABLED=1 CGO_CXXFLAGS="-std=c++17 -O2 -I$(PWD)/cpp" \
             CGO_LDFLAGS="-L$(PWD)/build/cpp -lkvstore_clib -lstdc++ -lm"

all: build

build: build/cpp/libkvstore_clib.a
	@echo "  Building Go server..."
	@mkdir -p bin
	@$(GOFLAGS) go build -ldflags="-s -w" -o ./bin/kvstore-server ./cmd/server/
	@echo "  ./bin/kvstore-server ready"

build/cpp/libkvstore_clib.a:
	@echo "  Building C++ library..."
	@mkdir -p build/cpp
	@$(CXX) -std=c++17 -O2 -fPIC -I./cpp \
	    -c ./cpp/kvstore_clib.cpp -o ./build/cpp/kvstore_clib.o
	@ar rcs ./build/cpp/libkvstore_clib.a ./build/cpp/kvstore_clib.o
	@echo "  libkvstore_clib.a ready"

test:
	@echo "  Running all tests..."
	@$(GOFLAGS) go test ./internal/... ./pkg/... ./tests/... \
	    -count=1 -timeout=120s -v 2>&1 | \
	    grep -E "PASS|FAIL|ok|---" | head -50

crash:
	@echo "  Running 50 crash recovery tests..."
	@$(GOFLAGS) go test ./tests/crash_recovery/... \
	    -v -count=1 -timeout=300s -parallel=8

bench:
	@echo "  Running benchmarks (30s each)..."
	@$(GOFLAGS) go test ./tests/ \
	    -bench="BenchmarkEngine_Write|BenchmarkEngine_Read" \
	    -benchtime=30s -benchmem -count=1

throughput:
	@$(GOFLAGS) go test ./tests/ -run=TestThroughput_200Clients -v -timeout=60s

fmt:
	@go fmt ./...

vet:
	@$(GOFLAGS) go vet ./...

run: build
	@./bin/kvstore-server --port 6380 --data-dir ./data --memtable-mb 4

clean:
	@rm -rf bin/ build/ data/
	@echo "cleaned"
