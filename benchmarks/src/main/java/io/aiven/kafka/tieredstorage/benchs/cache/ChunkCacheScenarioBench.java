/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.benchs.cache;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.DirectMemoryChunkCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

/**
 * Cache scenario benchmark simulating realistic Kafka tiered storage read patterns.
 *
 * <p>Segments are partitioned into HOT / WARM / COLD regions. HOT threads scan a small
 * hot region (tail consumers), WARM threads scan a larger warm region, COLD threads
 * sweep all segments sequentially, RANDOM threads do uniform random reads. Cache hits
 * emerge from thread overlap within regions.</p>
 *
 * <p>Two independent ratio sets control the simulation:</p>
 * <ul>
 *   <li><b>Region size</b> ({@code --hot-ratio}, {@code --warm-ratio}) — fraction of
 *       segments in HOT / WARM; the rest is COLD.</li>
 *   <li><b>Thread mix</b> ({@code --hot-thread-ratio}, {@code --warm-thread-ratio},
 *       {@code --cold-thread-ratio}, {@code --random-thread-ratio}) — fraction of threads
 *       per type. Normalised and assigned via largest-remainder so per-type counts sum
 *       to {@code --threads}.</li>
 * </ul>
 *
 * <p>The printed overlap ratio (HOT segs / HOT threads) shows whether a config will
 * actually produce hits. A pure uniform-random GC-stress run is the special case
 * {@code --random-thread-ratio 1.0 --prefill true}.</p>
 *
 * <p>Each operation reads {@code fetchChunks} consecutive chunks into one buffer,
 * mimicking a Kafka broker fetch spanning multiple chunks via FetchChunkEnumeration.</p>
 */
public class ChunkCacheScenarioBench {

    private static final int WARMUP_SECONDS = 30;
    private static final int REPORT_INTERVAL_SECONDS = 30;

    enum ConsumerType {
        /** Threads sharing a small hot region — tail consumers. */
        HOT,
        /** Reads the warm region (recent but not newest segments). */
        WARM,
        /** Sequential scan through all segments (catch-up / reprocessing). */
        COLD,
        /** Uniform random access across all segments. */
        RANDOM
    }

    private final Config config;
    private final ObjectKey[] objectKeys;
    private final SegmentManifest[] manifests;
    private final ChunkCache<?> cache;

    // Region boundaries: [0, warmStart) = cold, [warmStart, hotStart) = warm, [hotStart, total) = hot
    private final int hotStart;
    private final int warmStart;

    // Per-thread consumer type, indexed by threadId. Computed once via largest-remainder.
    private final ConsumerType[] assignment;

    private final AtomicLong[] opsByType = new AtomicLong[ConsumerType.values().length];
    private final AtomicLong totalOps = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final ConcurrentHashMap<ConsumerType, List<long[]>> latenciesByType = new ConcurrentHashMap<>();

    record Config(String cacheType, long cacheSize, int chunkSize,
                  int durationSeconds, int totalThreads, int totalSegments,
                  int chunksPerSegment, int missLatencyMs, int fetchChunks,
                  boolean prefill, String diskPath,
                  double hotRatio, double warmRatio,
                  double hotThreadRatio, double warmThreadRatio,
                  double coldThreadRatio, double randomThreadRatio) {
    }

    public ChunkCacheScenarioBench(final Config config) throws Exception {
        this.config = config;

        for (int i = 0; i < opsByType.length; i++) {
            opsByType[i] = new AtomicLong();
        }
        for (final ConsumerType ct : ConsumerType.values()) {
            latenciesByType.put(ct, new ArrayList<>());
        }

        // Validate up front so misconfiguration fails fast instead of producing
        // negative segment indices or a degenerate thread assignment.
        if (config.hotRatio() + config.warmRatio() >= 1.0) {
            throw new IllegalArgumentException(String.format(
                "hot-ratio + warm-ratio must be < 1.0 (leave room for COLD region): %.3f + %.3f",
                config.hotRatio(), config.warmRatio()));
        }

        // Region layout: [0, warmStart) = cold, [warmStart, hotStart) = warm, [hotStart, N) = hot
        final int hotSize = Math.max(1, (int) (config.totalSegments() * config.hotRatio()));
        final int warmSize = Math.max(1, (int) (config.totalSegments() * config.warmRatio()));
        this.hotStart = config.totalSegments() - hotSize;
        this.warmStart = this.hotStart - warmSize;

        this.assignment = buildAssignment();

        this.cache = createCache(config);
        this.objectKeys = new ObjectKey[config.totalSegments()];
        this.manifests = new SegmentManifest[config.totalSegments()];
        initSegments();
    }

    /**
     * Distributes {@code totalThreads} across the four consumer types via largest-remainder:
     * each type's right boundary is {@code round(cumulativeRatio * totalThreads)}; the last
     * type runs to {@code totalThreads} to absorb rounding drift. Threads are laid out
     * HOT, WARM, COLD, RANDOM by threadId.
     */
    private ConsumerType[] buildAssignment() {
        final ConsumerType[] types = ConsumerType.values();
        final double[] ratios = {
            config.hotThreadRatio(), config.warmThreadRatio(),
            config.coldThreadRatio(), config.randomThreadRatio(),
        };
        double sum = 0;
        for (final double r : ratios) {
            if (r < 0) {
                throw new IllegalArgumentException("thread ratios must be >= 0");
            }
            sum += r;
        }
        if (sum <= 0) {
            throw new IllegalArgumentException("at least one thread ratio must be > 0");
        }

        final int n = config.totalThreads();
        final ConsumerType[] result = new ConsumerType[n];
        int idx = 0;
        double cumulative = 0;
        for (int i = 0; i < types.length; i++) {
            cumulative += ratios[i] / sum;
            // Last type runs to n, absorbing rounding so the array is fully filled.
            final int end = (i == types.length - 1) ? n : (int) Math.round(cumulative * n);
            while (idx < end) {
                result[idx++] = types[i];
            }
        }
        return result;
    }

    private void initSegments() {
        final int segmentFileSize = config.chunkSize() * config.chunksPerSegment();
        final SegmentIndexesV1 indexes = SegmentIndexesV1.builder()
            .add(IndexType.OFFSET, 1)
            .add(IndexType.TIMESTAMP, 1)
            .add(IndexType.PRODUCER_SNAPSHOT, 1)
            .add(IndexType.LEADER_EPOCH, 1)
            .add(IndexType.TRANSACTION, 1)
            .build();

        for (int i = 0; i < config.totalSegments(); i++) {
            final String key = "tp-" + (i % 100) + "/seg-" + i;
            objectKeys[i] = () -> key;
            manifests[i] = new SegmentManifestV1(
                new FixedSizeChunkIndex(config.chunkSize(), segmentFileSize, config.chunkSize(), config.chunkSize()),
                indexes, false, null, null);
        }
    }

    private static ChunkCache<?> createCache(final Config config) throws Exception {
        final byte[] templateData = new byte[config.chunkSize()];
        ThreadLocalRandom.current().nextBytes(templateData);

        final ChunkManager mockChunkManager = (objectKey, manifest, chunkId) -> {
            if (config.missLatencyMs() > 0) {
                try {
                    Thread.sleep(config.missLatencyMs());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return new ByteArrayInputStream(templateData);
        };

        final Path diskPath = config.diskPath() != null ? Path.of(config.diskPath()) : null;
        return CacheBenchUtils.createCache(
            config.cacheType(), mockChunkManager, config.chunkSize(), config.cacheSize(), diskPath);
    }

    public void run() throws Exception {
        final int warmupSeconds = Math.min(WARMUP_SECONDS, config.durationSeconds() / 3);
        final int measuredSeconds = config.durationSeconds() - warmupSeconds;

        printHeader(warmupSeconds, measuredSeconds);
        if (config.prefill()) {
            prefillCache();
        }

        final long startNano = System.nanoTime();
        final long deadline = startNano + (long) config.durationSeconds() * 1_000_000_000L;
        final long warmupEnd = startNano + (long) warmupSeconds * 1_000_000_000L;

        final long[] gcAtWarmupEnd = runBenchmark(deadline, warmupEnd, startNano);
        printResults(warmupSeconds, measuredSeconds, startNano, gcAtWarmupEnd);
    }

    private long[] runBenchmark(final long deadline, final long warmupEnd, final long startNano)
        throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(config.totalThreads());
        final long gcCountBefore = CacheBenchUtils.totalGcCount();
        final long gcTimeBefore = CacheBenchUtils.totalGcTimeMs();
        final long[] gcAtWarmupEnd = new long[2];

        // Launch consumer threads
        for (int t = 0; t < config.totalThreads(); t++) {
            final ConsumerType type = assignConsumerType(t);
            final int threadId = t;
            new Thread(() -> {
                try {
                    runConsumer(type, threadId, deadline, warmupEnd);
                } catch (final Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    done.countDown();
                }
            }, "consumer-" + t + "-" + type).start();
        }

        // Snapshot GC at warmup end
        final long sleepForWarmup = (warmupEnd - System.nanoTime()) / 1_000_000L;
        if (sleepForWarmup > 0) {
            Thread.sleep(sleepForWarmup);
        }
        gcAtWarmupEnd[0] = CacheBenchUtils.totalGcCount() - gcCountBefore;
        gcAtWarmupEnd[1] = CacheBenchUtils.totalGcTimeMs() - gcTimeBefore;

        startReporter(deadline, warmupEnd, startNano);
        done.await();
        return gcAtWarmupEnd;
    }

    private void startReporter(final long deadline, final long warmupEnd, final long startNano) {
        final Thread reporter = new Thread(() -> {
            try {
                while (System.nanoTime() < deadline) {
                    Thread.sleep(REPORT_INTERVAL_SECONDS * 1000L);
                    final long now = System.nanoTime();
                    final long elapsed = Math.max(1, (now - warmupEnd) / 1_000_000_000L);
                    final long ops = totalOps.get();
                    System.out.printf("[%3ds] ops=%d, throughput=%d ops/s, GC count=%d, GC time=%dms%n",
                        (now - startNano) / 1_000_000_000L, ops, ops / elapsed,
                        CacheBenchUtils.totalGcCount(), CacheBenchUtils.totalGcTimeMs());
                }
            } catch (final InterruptedException ignored) {
                // reporter shutdown
            }
        }, "reporter");
        reporter.setDaemon(true);
        reporter.start();
    }

    private void runConsumer(final ConsumerType type, final int threadId,
                             final long deadline, final long warmupEnd) throws Exception {
        final ByteBuffer buf = ByteBuffer.allocate(config.chunkSize() * config.fetchChunks());
        long[] localLatencies = new long[200000];
        int latencyIdx = 0;

        int segIdx = pickInitialSegment(type, threadId);
        int chunkIdx = 0;

        while (System.nanoTime() < deadline) {
            final int seg;
            final int startChunk;

            if (type == ConsumerType.RANDOM) {
                seg = ThreadLocalRandom.current().nextInt(config.totalSegments());
                startChunk = ThreadLocalRandom.current().nextInt(
                    Math.max(1, config.chunksPerSegment() - config.fetchChunks() + 1));
            } else {
                seg = segIdx;
                startChunk = chunkIdx;
                chunkIdx += config.fetchChunks();
                if (chunkIdx >= config.chunksPerSegment()) {
                    chunkIdx = 0;
                    segIdx = pickNextSegment(type, segIdx);
                }
            }

            final long opStart = System.nanoTime();
            readChunks(seg, startChunk, buf);
            final long opEnd = System.nanoTime();

            // Skip warmup period
            if (opEnd >= warmupEnd) {
                totalOps.incrementAndGet();
                opsByType[type.ordinal()].incrementAndGet();
                if (latencyIdx >= localLatencies.length) {
                    localLatencies = Arrays.copyOf(localLatencies, localLatencies.length * 2);
                }
                localLatencies[latencyIdx++] = (opEnd - opStart) / 1000L; // nanos -> micros
            }
        }

        synchronized (latenciesByType.get(type)) {
            latenciesByType.get(type).add(Arrays.copyOf(localLatencies, latencyIdx));
        }
    }

    /**
     * Reads consecutive chunks into a single buffer, mirroring the broker's
     * {@code Utils.readFully(remoteSegInputStream, fetchBuffer)} loop in
     * {@code RemoteLogManager.read()}.
     */
    private void readChunks(final int seg, final int startChunk, final ByteBuffer buf) throws Exception {
        final int endChunk = Math.min(startChunk + config.fetchChunks(), config.chunksPerSegment());
        buf.clear();
        for (int c = startChunk; c < endChunk; c++) {
            if (!buf.hasRemaining()) {
                break;
            }
            try (InputStream is = cache.getChunk(objectKeys[seg], manifests[seg], c)) {
                Utils.readFully(is, buf);
            }
        }
    }

    private int pickInitialSegment(final ConsumerType type, final int threadId) {
        return switch (type) {
            case HOT -> hotStart + ThreadLocalRandom.current().nextInt(config.totalSegments() - hotStart);
            case WARM -> warmStart + ThreadLocalRandom.current().nextInt(hotStart - warmStart);
            case COLD -> threadId % Math.max(1, warmStart);
            default -> 0;
        };
    }

    private int pickNextSegment(final ConsumerType type, final int currentSeg) {
        return switch (type) {
            case HOT -> currentSeg + 1 >= config.totalSegments() ? hotStart : currentSeg + 1;
            case WARM -> currentSeg + 1 >= hotStart ? warmStart : currentSeg + 1;
            case COLD -> (currentSeg + 1) % config.totalSegments();
            default -> currentSeg;
        };
    }

    private ConsumerType assignConsumerType(final int threadId) {
        return assignment[threadId];
    }

    private void printHeader(final int warmupSeconds, final int measuredSeconds) {
        final long workingSetMb =
            (long) config.totalSegments() * config.chunksPerSegment() * config.chunkSize() / (1024 * 1024);

        // Per-type thread counts from the resolved assignment.
        final int[] threadCounts = new int[ConsumerType.values().length];
        for (final ConsumerType ct : assignment) {
            threadCounts[ct.ordinal()]++;
        }
        final int hotThreads = threadCounts[ConsumerType.HOT.ordinal()];
        final int warmThreads = threadCounts[ConsumerType.WARM.ordinal()];
        final int hotSegs = config.totalSegments() - hotStart;
        final int warmSegs = hotStart - warmStart;

        System.out.printf("=== Region-Based Cache Scenario ===%n");
        System.out.printf("Segments: %d (%d chunks each), Working set: %dMB%n",
            config.totalSegments(), config.chunksPerSegment(), workingSetMb);
        System.out.printf("Regions: HOT[%d-%d] %d segs / %d threads, WARM[%d-%d] %d segs / %d threads, "
                + "COLD[0-%d] %d segs / %d threads, RANDOM %d threads%n",
            hotStart, config.totalSegments() - 1, hotSegs, hotThreads,
            warmStart, hotStart - 1, warmSegs, warmThreads,
            warmStart - 1, warmStart, threadCounts[ConsumerType.COLD.ordinal()],
            threadCounts[ConsumerType.RANDOM.ordinal()]);
        // Overlap = segments per thread within a region. < 1.0 means threads share
        // segments (hits emerge from overlap); >> 1.0 means too sparse for sharing.
        System.out.printf("Overlap (segs/thread): HOT %.3f, WARM %.3f  (lower = more sharing = higher hit rate)%n",
            hotThreads > 0 ? (double) hotSegs / hotThreads : Double.NaN,
            warmThreads > 0 ? (double) warmSegs / warmThreads : Double.NaN);
        System.out.printf("Threads: %d, Duration: %ds (warmup: %ds, measurement: %ds)%n",
            config.totalThreads(), config.durationSeconds(), warmupSeconds, measuredSeconds);
        System.out.printf("Prefill: %b, Miss latency: %dms, Fetch chunks: %d%n%n",
            config.prefill(), config.missLatencyMs(), config.fetchChunks());
    }

    private void prefillCache() throws Exception {
        // Prefill only fully warms the cache when the working set fits. If it exceeds
        // cacheSize, Caffeine evicts during the sweep and only the last ~cacheSize entries
        // survive, so the run still misses heavily. Warn instead of silently misleading.
        final long workingSet = (long) config.totalSegments() * config.chunksPerSegment() * config.chunkSize();
        if (workingSet > config.cacheSize()) {
            System.out.printf("WARNING: working set %dMB > cache %dMB — prefill cannot fully warm the cache, "
                    + "subsequent reads will still miss. Reduce --segments/--chunks-per-segment so that "
                    + "segments*chunksPerSegment*chunkSize <= cache-size.%n",
                workingSet / (1024 * 1024), config.cacheSize() / (1024 * 1024));
        }
        System.out.printf("Pre-filling cache...%n");
        for (int i = 0; i < config.totalSegments(); i++) {
            for (int c = 0; c < config.chunksPerSegment(); c++) {
                try (InputStream is = cache.getChunk(objectKeys[i], manifests[i], c)) {
                    is.readAllBytes();
                }
            }
        }
        System.out.printf("Pre-fill done (%d entries)%n%n",
            config.totalSegments() * config.chunksPerSegment());
    }

    private void printResults(final int warmupSeconds, final int measuredSeconds,
                              final long startNano, final long[] gcDuringWarmup) {
        final long measuredOps = totalOps.get();
        final long gcCount = CacheBenchUtils.totalGcCount();
        final long gcTime = CacheBenchUtils.totalGcTimeMs();

        System.out.printf("%n=== Results (excluding %ds warmup) ===%n", warmupSeconds);
        System.out.printf("Total ops:       %d%n", measuredOps);
        System.out.printf("Throughput:      %d ops/s%n", measuredOps / Math.max(1, measuredSeconds));
        System.out.printf("GC count:        %d%n", gcCount);
        System.out.printf("GC time (ms):    %d%n", gcTime);
        if (measuredOps > 0) {
            System.out.printf("GC time/op:      %.4f ms%n", (double) gcTime / measuredOps);
        }
        System.out.printf("Warmup GC:       count=%d time=%dms%n", gcDuringWarmup[0], gcDuringWarmup[1]);

        if (errorCount.get() > 0) {
            System.out.printf("%n*** RESULT MAY BE INVALID: %d thread(s) failed ***%n", errorCount.get());
        }

        // Per-type breakdown
        System.out.printf("%nOps by consumer type:%n");
        for (final ConsumerType ct : ConsumerType.values()) {
            System.out.printf("  %-8s %d%n", ct, opsByType[ct.ordinal()].get());
        }

        // GC collector details
        System.out.printf("%nGC collectors:%n");
        for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            System.out.printf("  %s: count=%d time=%dms%n",
                gc.getName(), gc.getCollectionCount(), gc.getCollectionTime());
        }

        printCacheStats();
        printLatencyStats();
    }

    private void printCacheStats() {
        final var stats = cache.cacheStats();
        System.out.printf("%nCache stats:%n");
        System.out.printf("  Hit count:       %d%n", stats.hitCount());
        System.out.printf("  Miss count:      %d%n", stats.missCount());
        System.out.printf("  Hit rate:        %.2f%%%n",
            stats.hitCount() * 100.0 / Math.max(1, stats.requestCount()));
        System.out.printf("  Eviction count:  %d%n", stats.evictionCount());
        System.out.printf("  Eviction weight: %d bytes (%.1f MB)%n",
            stats.evictionWeight(), stats.evictionWeight() / (1024.0 * 1024.0));
        System.out.printf("  Estimated size:  %d entries%n", cache.estimatedSize());

        if (cache instanceof DirectMemoryChunkCache) {
            System.out.printf("  Buffer pool:     %d buffers pooled%n",
                ((DirectMemoryChunkCache) cache).getBufferPool().currentPoolSize());
        }
    }

    private void printLatencyStats() {
        System.out.printf("%nLatency (us) by consumer type:%n");
        System.out.printf("  %-8s %10s %10s %10s %10s %10s%n",
            "Type", "p50", "p95", "p99", "p99.9", "max");

        final List<long[]> allMerged = new ArrayList<>();
        for (final ConsumerType ct : ConsumerType.values()) {
            final long[] merged = mergeLatencies(latenciesByType.get(ct));
            if (merged.length == 0) {
                continue;
            }
            Arrays.sort(merged);
            System.out.printf("  %-8s %10d %10d %10d %10d %10d%n", ct,
                percentile(merged, 50), percentile(merged, 95),
                percentile(merged, 99), percentile(merged, 99.9),
                merged[merged.length - 1]);
            allMerged.add(merged);
        }
        final long[] allLatencies = mergeLatencies(allMerged);
        if (allLatencies.length > 0) {
            Arrays.sort(allLatencies);
            System.out.printf("  %-8s %10d %10d %10d %10d %10d%n", "ALL",
                percentile(allLatencies, 50), percentile(allLatencies, 95),
                percentile(allLatencies, 99), percentile(allLatencies, 99.9),
                allLatencies[allLatencies.length - 1]);
        }
    }

    private static long[] mergeLatencies(final List<long[]> arrays) {
        int total = 0;
        for (final long[] a : arrays) {
            total += a.length;
        }
        final long[] result = new long[total];
        int pos = 0;
        for (final long[] a : arrays) {
            System.arraycopy(a, 0, result, pos, a.length);
            pos += a.length;
        }
        return result;
    }

    private static long percentile(final long[] sorted, final double p) {
        final int idx = Math.min((int) (sorted.length * p / 100.0), sorted.length - 1);
        return sorted[idx];
    }

    public static void main(final String[] args) throws Exception {
        final Config config = parseArgs(args);
        System.out.printf("Cache type: %s, cache size: %dMB, chunk size: %dMB%n",
            config.cacheType(), config.cacheSize() / (1024 * 1024),
            config.chunkSize() / (1024 * 1024));

        new ChunkCacheScenarioBench(config).run();
    }

    private static Config parseArgs(final String[] args) {
        if (args.length < 1 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            System.err.println("Usage: ChunkCacheScenarioBench <cacheType> [options]");
            System.err.println("  Cache types: memory, refcount-direct, disk");
            System.err.println("  --cache-size N         bytes (default: 268435456 = 256MB)");
            System.err.println("  --chunk-size N         bytes (default: 4194304 = 4MB)");
            System.err.println("  --duration N           seconds (default: 300)");
            System.err.println("  --threads N            (default: 16)");
            System.err.println("  --segments N           (default: 512)");
            System.err.println("  --chunks-per-segment N (default: 64)");
            System.err.println("  --miss-latency-ms N    simulated remote latency (default: 0)");
            System.err.println("  --fetch-chunks N       chunks per fetch op (default: 1)");
            System.err.println("  --disk-path PATH       disk cache directory (default: auto temp dir)");
            System.err.println("  --hot-ratio N          fraction of segments in hot region (default: 0.1)");
            System.err.println("  --warm-ratio N         fraction of segments in warm region (default: 0.2)");
            System.err.println("  --hot-thread-ratio N     fraction of threads scanning hot region (default: 0.625)");
            System.err.println("  --warm-thread-ratio N    fraction of threads scanning warm region (default: 0.25)");
            System.err.println("  --cold-thread-ratio N    fraction of threads sweeping segments (default: 0.0625)");
            System.err.println("  --random-thread-ratio N  fraction of threads doing random reads (default: 0.0625)");
            System.err.println("  --prefill true|false   warm the whole cache before measuring (default: false)");
            System.exit(1);
        }

        final String cacheType = args[0];
        long cacheSize = 268435456;
        int chunkSize = 4194304;
        int duration = 300;
        int threads = 16;
        int segments = 512;
        int chunksPerSegment = 64;
        int missLatencyMs = 0;
        int fetchChunks = 1;
        boolean prefill = false;
        String diskPath = null;
        double hotRatio = 0.1;
        double warmRatio = 0.2;
        // Defaults reproduce the previous hard-coded 16-thread mix: 10 HOT / 4 WARM / 1 COLD / 1 RANDOM.
        double hotThreadRatio = 0.625;
        double warmThreadRatio = 0.25;
        double coldThreadRatio = 0.0625;
        double randomThreadRatio = 0.0625;

        for (int i = 1; i < args.length - 1; i += 2) {
            switch (args[i]) {
                case "--cache-size":
                    cacheSize = Long.parseLong(args[i + 1]);
                    break;
                case "--chunk-size":
                    chunkSize = Integer.parseInt(args[i + 1]);
                    break;
                case "--duration":
                    duration = Integer.parseInt(args[i + 1]);
                    break;
                case "--threads":
                    threads = Integer.parseInt(args[i + 1]);
                    break;
                case "--segments":
                    segments = Integer.parseInt(args[i + 1]);
                    break;
                case "--chunks-per-segment":
                    chunksPerSegment = Integer.parseInt(args[i + 1]);
                    break;
                case "--miss-latency-ms":
                    missLatencyMs = Integer.parseInt(args[i + 1]);
                    break;
                case "--fetch-chunks":
                    fetchChunks = Integer.parseInt(args[i + 1]);
                    break;
                case "--disk-path":
                    diskPath = args[i + 1];
                    break;
                case "--hot-ratio":
                    hotRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--warm-ratio":
                    warmRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--hot-thread-ratio":
                    hotThreadRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--warm-thread-ratio":
                    warmThreadRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--cold-thread-ratio":
                    coldThreadRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--random-thread-ratio":
                    randomThreadRatio = Double.parseDouble(args[i + 1]);
                    break;
                case "--prefill":
                    prefill = Boolean.parseBoolean(args[i + 1]);
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
            }
        }

        return new Config(cacheType, cacheSize, chunkSize, duration, threads,
            segments, chunksPerSegment, missLatencyMs, fetchChunks, prefill,
            diskPath, hotRatio, warmRatio,
            hotThreadRatio, warmThreadRatio, coldThreadRatio, randomThreadRatio);
    }
}
