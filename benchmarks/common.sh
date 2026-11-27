#!/bin/bash

HEAP_SIZE="${HEAP_SIZE:-6g}"
# memory cache stores chunks on heap, so it needs a larger heap than other types.
MEMORY_HEAP_SIZE="${MEMORY_HEAP_SIZE:-12g}"
DIRECT_MEM_SIZE="${DIRECT_MEM_SIZE:-8g}"
G1_REGION_SIZE="${G1_REGION_SIZE-16m}"
THREADS="${THREADS:-16}"
CACHE_SIZE="${CACHE_SIZE:-6442450944}"
CHUNK_SIZE="${CHUNK_SIZE:-4194304}"
FETCH_CHUNKS="${FETCH_CHUNKS:-4}"

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/.." && pwd)"

build_jvm_opts() {
    local heap="${1:-$HEAP_SIZE}"
    local opts="-Xmx${heap} -XX:+UseG1GC -XX:MaxDirectMemorySize=${DIRECT_MEM_SIZE}"
    if [ -n "$G1_REGION_SIZE" ]; then
        opts="$opts -XX:G1HeapRegionSize=${G1_REGION_SIZE}"
    fi
    echo "$opts"
}

build_jar() {
    echo "=== Building shadow JAR ==="
    (cd "$PROJECT_DIR" && ./gradlew :benchmarks:shadowJar -q)
    JAR=$(ls "$PROJECT_DIR"/benchmarks/build/libs/kafka-ts-benchmarks-*-all.jar | head -1)
    echo "JAR: $JAR"
}

# Create a timestamped results dir; sets global RESULTS_DIR. Arg: tag prefix.
make_results_dir() {
    local tag="${1:-bench}"
    local run_id="$(date +%Y%m%d_%H%M%S)_${tag}_heap${HEAP_SIZE}_cache${CACHE_SIZE}_chunk${CHUNK_SIZE}_t${THREADS}"
    RESULTS_DIR="$PROJECT_DIR/benchmarks/results/${run_id}"
    mkdir -p "$RESULTS_DIR"
    echo "Results: $RESULTS_DIR"
}
