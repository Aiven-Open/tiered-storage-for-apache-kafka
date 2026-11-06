#!/bin/bash
# Scenario benchmark driver (non-JMH; runs ChunkCacheScenarioBench main directly).
# Usage: ./benchmarks/scenario.sh
#
# For a pure uniform-random GC stress run, zero out the other three thread ratios:
#   HOT_THREAD_RATIO=0 WARM_THREAD_RATIO=0 COLD_THREAD_RATIO=0 RANDOM_THREAD_RATIO=1 \
#     PREFILL=true ./benchmarks/scenario.sh
#
# Each cacheType runs in its own JVM, so heap can be sized per type — memory uses
# MEMORY_HEAP_SIZE, others use HEAP_SIZE.

set -euo pipefail

source "$(dirname "$0")/common.sh"

SCENARIO_MAIN="io.aiven.kafka.tieredstorage.benchs.cache.ChunkCacheScenarioBench"

# Scenario-specific config (env-overridable). Shared knobs live in common.sh.
SEGMENTS="${SEGMENTS:-512}"
CHUNKS_PER_SEG="${CHUNKS_PER_SEG:-64}"
DURATION="${DURATION:-600}"
MISS_LATENCY_MS="${MISS_LATENCY_MS:-0}"
HOT_RATIO="${HOT_RATIO:-0.1}"
WARM_RATIO="${WARM_RATIO:-0.2}"
DISK_PATH="${DISK_PATH:-}"

# Thread mix; defaults reproduce the original 16-thread 10/4/1/1 split.
HOT_THREAD_RATIO="${HOT_THREAD_RATIO:-10}"
WARM_THREAD_RATIO="${WARM_THREAD_RATIO:-4}"
COLD_THREAD_RATIO="${COLD_THREAD_RATIO:-1}"
RANDOM_THREAD_RATIO="${RANDOM_THREAD_RATIO:-1}"

PREFILL="${PREFILL:-false}"

run_scenario() {
    # With prefill, size the working set to exactly fit the cache so every entry stays
    # warm and the run is pure cache-hit; otherwise eviction during prefill makes it
    # meaningless. Without prefill, use configured SEGMENTS/CHUNKS_PER_SEG to drive eviction.
    local seg=$SEGMENTS cps=$CHUNKS_PER_SEG
    if [ "$PREFILL" = "true" ]; then
        seg=$((CACHE_SIZE / CHUNK_SIZE)); cps=1
    fi

    echo "=== ChunkCacheScenarioBench region (threadMix=$HOT_THREAD_RATIO/$WARM_THREAD_RATIO/$COLD_THREAD_RATIO/$RANDOM_THREAD_RATIO prefill=$PREFILL segments=$seg chunks=$cps, ${DURATION}s each) ==="

    local disk_opt=""
    [ -n "$DISK_PATH" ] && mkdir -p "$DISK_PATH" && disk_opt="--disk-path $DISK_PATH"

    for type in memory refcount-direct disk; do
        echo "--- $type ---"

        local heap=$HEAP_SIZE
        [ "$type" = "memory" ] && heap=$MEMORY_HEAP_SIZE

        java $(build_jvm_opts "$heap") \
            -Xlog:gc*:file="$RESULTS_DIR/region_gc_${type}.log":time,tags \
            -cp "$JAR" \
            "$SCENARIO_MAIN" \
            "$type" --cache-size $CACHE_SIZE --chunk-size $CHUNK_SIZE \
            --duration $DURATION --threads $THREADS \
            --segments $seg --chunks-per-segment $cps \
            --miss-latency-ms $MISS_LATENCY_MS --fetch-chunks $FETCH_CHUNKS \
            --hot-ratio $HOT_RATIO --warm-ratio $WARM_RATIO \
            --hot-thread-ratio $HOT_THREAD_RATIO --warm-thread-ratio $WARM_THREAD_RATIO \
            --cold-thread-ratio $COLD_THREAD_RATIO --random-thread-ratio $RANDOM_THREAD_RATIO \
            --prefill $PREFILL \
            $disk_opt \
            | tee "$RESULTS_DIR/region_${type}.txt"
        echo ""
    done
}

build_jar
make_results_dir region
echo "Config: heap=$HEAP_SIZE memHeap=$MEMORY_HEAP_SIZE direct=$DIRECT_MEM_SIZE region=${G1_REGION_SIZE:-JVM-default} threads=$THREADS cache=$CACHE_SIZE chunk=$CHUNK_SIZE"

run_scenario

echo "=== Done. Results in $RESULTS_DIR/ ==="
