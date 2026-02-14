#!/usr/bin/env bash
# scripts/build.sh
# Builds the ConcurrentKV project: C++ library + Go binary together.
#
# Requirements:
#   - Go 1.22+
#   - g++ with C++17 support (or clang++)
#   - CGO_ENABLED=1 (default on Linux/macOS)
#
# Output: ./bin/kvstore-server

set -euo pipefail
cd "$(dirname "$0")/.."

echo "══════════════════════════════════════════════"
echo "  ConcurrentKV — Build (Go + C++17)"
echo "══════════════════════════════════════════════"

# ── Detect compiler ────────────────────────────────────────────────────────────
CXX="${CXX:-g++}"
if ! command -v "$CXX" &>/dev/null; then
    CXX="clang++"
    if ! command -v "$CXX" &>/dev/null; then
        echo "❌  No C++ compiler found. Install g++ or clang++."
        exit 1
    fi
fi
echo "  C++ compiler : $CXX ($($CXX --version | head -1))"

# ── Build C++ static library ───────────────────────────────────────────────────
echo ""
echo "📦  Building C++ library (Bloom filter + SSTable writer)..."
mkdir -p build/cpp

$CXX -std=c++17 -O2 -fPIC \
    -I./cpp \
    -c ./cpp/kvstore_clib.cpp \
    -o ./build/cpp/kvstore_clib.o

ar rcs ./build/cpp/libkvstore_clib.a ./build/cpp/kvstore_clib.o
echo "  ✅  libkvstore_clib.a built"

# ── Build Go binary ───────────────────────────────────────────────────────────
echo ""
echo "🐹  Building Go server (CGO_ENABLED=1)..."
mkdir -p bin

CGO_ENABLED=1 \
CGO_CXXFLAGS="-std=c++17 -O2 -I$(pwd)/cpp" \
CGO_LDFLAGS="-L$(pwd)/build/cpp -lkvstore_clib -lstdc++ -lm" \
go build \
    -ldflags="-s -w" \
    -o ./bin/kvstore-server \
    ./cmd/server/

echo "  ✅  ./bin/kvstore-server built"

# ── Run unit tests ────────────────────────────────────────────────────────────
echo ""
echo "🧪  Running unit tests..."
CGO_ENABLED=1 \
CGO_CXXFLAGS="-std=c++17 -O2 -I$(pwd)/cpp" \
CGO_LDFLAGS="-L$(pwd)/build/cpp -lkvstore_clib -lstdc++ -lm" \
go test ./internal/... ./pkg/... -count=1 -timeout=120s 2>&1 | \
    sed 's/^/  /'

echo ""
echo "══════════════════════════════════════════════"
echo "  ✅  Build complete!"
echo ""
echo "  Start server:     ./bin/kvstore-server --port 6380"
echo "  Run benchmarks:   ./scripts/benchmark.sh"
echo "  Crash recovery:   go test ./tests/crash_recovery/... -v"
echo "══════════════════════════════════════════════"
