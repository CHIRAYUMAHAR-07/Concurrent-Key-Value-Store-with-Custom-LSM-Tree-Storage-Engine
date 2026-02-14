#!/usr/bin/env bash
# scripts/benchmark.sh
# Runs the full benchmark suite:
#   1. Engine-level benchmark (no TCP overhead)
#   2. TCP server benchmark (200 clients)
#   3. Bloom filter effectiveness test
#   4. Crash recovery verification

set -euo pipefail
cd "$(dirname "$0")/.."

echo "══════════════════════════════════════════════════════════"
echo "  ConcurrentKV — Benchmark Suite"
echo "══════════════════════════════════════════════════════════"
echo ""

# ── 1. Engine-level Go benchmarks ────────────────────────────────────────────
echo "📊 1. Engine throughput (go test -bench)..."
go test ./tests/ \
    -bench="BenchmarkEngine_Write|BenchmarkEngine_Read" \
    -benchtime=30s \
    -benchmem \
    -count=1 \
    -cpu=8 \
    2>&1 | grep -E "Benchmark|ops/sec|ns/op|allocs" | \
    awk '{printf "  %s\n", $0}'

echo ""

# ── 2. Bloom filter test ──────────────────────────────────────────────────────
echo "🔍 2. Bloom filter negative lookup reduction..."
go test ./tests/ \
    -bench="BenchmarkBloomFilter" \
    -benchtime=10s \
    -count=1 \
    2>&1 | grep -E "Benchmark|bloom|filtered" | \
    awk '{printf "  %s\n", $0}'

echo ""

# ── 3. 200-client throughput test ────────────────────────────────────────────
echo "👥 3. 200 concurrent client throughput test..."
go test ./tests/ \
    -run="TestThroughput_200Clients" \
    -v \
    -timeout=60s \
    2>&1 | grep -E "═|client|Write|Read|ops|stat" | \
    awk '{printf "  %s\n", $0}'

echo ""

# ── 4. Crash recovery ────────────────────────────────────────────────────────
echo "💥 4. Crash recovery tests (50 hard-kill scenarios)..."
go test ./tests/crash_recovery/ \
    -v \
    -count=1 \
    -timeout=300s \
    2>&1 | grep -E "PASS|FAIL|✅|MISS|zero|Scenario" | head -60 | \
    awk '{printf "  %s\n", $0}'

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Benchmark complete!"
echo "══════════════════════════════════════════════════════════"
