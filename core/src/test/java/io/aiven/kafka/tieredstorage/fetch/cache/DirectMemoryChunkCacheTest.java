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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.fetch.ChunkKey;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DirectMemoryChunkCacheTest {
    private static final int CHUNK_SIZE = 10;
    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "abcdefghij".getBytes();
    private static final ObjectKey SEGMENT_OBJECT_KEY = () -> "topic/segment";
    private static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();
    private static final SegmentManifest SEGMENT_MANIFEST = new SegmentManifestV1(
        new FixedSizeChunkIndex(CHUNK_SIZE, CHUNK_SIZE, CHUNK_SIZE, CHUNK_SIZE),
        SEGMENT_INDEXES,
        false,
        null,
        null
    );
    private static final SegmentManifest TWO_CHUNK_MANIFEST = new SegmentManifestV1(
        new FixedSizeChunkIndex(CHUNK_SIZE, CHUNK_SIZE * 2, CHUNK_SIZE, CHUNK_SIZE),
        SEGMENT_INDEXES,
        false,
        null,
        null
    );

    @Mock
    private ChunkManager chunkManager;

    private DirectMemoryChunkCache chunkCache;

    @Test
    void cacheMissReadsFromChunkManager() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));

        chunkCache = newCache(cacheConfig());

        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)) {
            assertThat(chunk).hasBinaryContent(CHUNK_0);
        }

        final var stats = chunkCache.cacheStats();
        assertThat(stats.missCount()).isEqualTo(1);
        assertThat(stats.hitCount()).isZero();
    }

    @Test
    void cacheHitReturnsCachedDataWithoutRefetching() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));

        chunkCache = newCache(cacheConfig());

        try (InputStream first = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)) {
            assertThat(first).hasBinaryContent(CHUNK_0);
        }
        try (InputStream second = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)) {
            assertThat(second).hasBinaryContent(CHUNK_0);
        }

        verify(chunkManager, times(1)).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
        assertThat(chunkCache.cacheStats().hitCount()).isEqualTo(1);
    }

    @Test
    void weigherReturnsBufferCapacity() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));

        chunkCache = newCache(cacheConfig());

        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)) {
            assertThat(chunk).hasBinaryContent(CHUNK_0);
        }

        // The weigher drives size-based eviction; it must report the buffer capacity.
        final ChunkKey key = new ChunkKey(SEGMENT_OBJECT_KEY.value(), 0);
        final RefCountedByteBuffer cached = chunkCache.cache.synchronous().getIfPresent(key);
        assertThat(cached).isNotNull();
        assertThat(chunkCache.weigher().weigh(key, cached)).isEqualTo(CHUNK_SIZE);
    }

    @Test
    void oversizedChunkAllocatesLargerBufferThatBypassesPool() throws Exception {
        final byte[] oversizedData = "01234567890123456789".getBytes();
        final SegmentManifest oversizedManifest = new SegmentManifestV1(
            new FixedSizeChunkIndex(20, 20, 20, 20),
            SEGMENT_INDEXES,
            false,
            null,
            null
        );
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, oversizedManifest, 0))
            .thenReturn(new ByteArrayInputStream(oversizedData));

        chunkCache = newCache(Map.of(
            "retention.ms", "-1",
            "size", "100",
            "chunk.size", String.valueOf(CHUNK_SIZE),
            "thread.pool.size", "1"
        ));

        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, oversizedManifest, 0)) {
            assertThat(chunk).hasBinaryContent(oversizedData);
        }

        // Buffer larger than chunk.size must not be pooled on release.
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
            assertThat(chunkCache.getBufferPool().currentPoolSize()).isZero());
    }

    @Test
    void evictionReleasesBufferToPool() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 1))
            .thenReturn(new ByteArrayInputStream(CHUNK_1));

        // Cache holds a single chunk; the second read evicts the first.
        chunkCache = newCache(cacheConfig());

        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 0)) {
            assertThat(chunk).hasBinaryContent(CHUNK_0);
        }
        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 1)) {
            assertThat(chunk).hasBinaryContent(CHUNK_1);
        }

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
            assertThat(chunkCache.getBufferPool().currentPoolSize()).isPositive());
    }

    @Test
    void refCountPreventsPoolReturnWhileStreamOpen() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 1))
            .thenReturn(new ByteArrayInputStream(CHUNK_1));

        chunkCache = newCache(cacheConfig());

        final InputStream openStream = chunkCache.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 0);

        // Reading the second chunk evicts the first, but its buffer must stay out of the
        // pool while the first stream is still open (otherwise: use-after-free).
        try (InputStream chunk = chunkCache.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 1)) {
            assertThat(chunk).hasBinaryContent(CHUNK_1);
        }

        await().atMost(Duration.ofSeconds(2)).pollDelay(Duration.ofMillis(100)).untilAsserted(() ->
            assertThat(chunkCache.estimatedSize()).isLessThanOrEqualTo(1));
        assertThat(chunkCache.getBufferPool().currentPoolSize()).isZero();

        openStream.close();

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
            assertThat(chunkCache.getBufferPool().currentPoolSize()).isEqualTo(1));
    }

    @Test
    void concurrentReadersOnSameKeyReleaseBufferExactlyOnce() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));

        // Single-entry cache; many readers retain/release the same buffer concurrently.
        chunkCache = newCache(cacheConfig());

        final int threadCount = 8;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final Thread t = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int r = 0; r < 50; r++) {
                        try (InputStream chunk =
                                 chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)) {
                            assertThat(chunk).hasBinaryContent(CHUNK_0);
                        }
                    }
                } catch (final Throwable e) {
                    errors.add(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (final Thread t : threads) {
            t.join(15_000);
        }

        assertThat(errors).isEmpty();
        // Entry still cached and held by exactly one (cache) reference: not yet in the pool.
        assertThat(chunkCache.getBufferPool().currentPoolSize()).isZero();
        verify(chunkManager, times(1)).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
    }

    @Test
    void concurrentReadsOnDifferentKeys() throws Exception {
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(CHUNK_0));
        when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, 1))
            .thenReturn(new ByteArrayInputStream(CHUNK_1));

        chunkCache = newCache(Map.of(
            "retention.ms", "-1",
            "size", String.valueOf(CHUNK_SIZE * 2),
            "chunk.size", String.valueOf(CHUNK_SIZE),
            "thread.pool.size", "2"
        ));

        final int threadCount = 2;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int chunkId = i;
            final byte[] expected = chunkId == 0 ? CHUNK_0 : CHUNK_1;
            final Thread t = new Thread(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    try (InputStream chunk = chunkCache.getChunk(
                        SEGMENT_OBJECT_KEY, TWO_CHUNK_MANIFEST, chunkId)) {
                        assertThat(chunk).hasBinaryContent(expected);
                    }
                } catch (final Throwable e) {
                    errors.add(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (final Thread t : threads) {
            t.join(10_000);
        }

        assertThat(errors).isEmpty();
    }

    private DirectMemoryChunkCache newCache(final Map<String, ?> config) {
        final DirectMemoryChunkCache cache = new DirectMemoryChunkCache(chunkManager);
        cache.configure(config);
        return cache;
    }

    private static Map<String, String> cacheConfig() {
        return Map.of(
            "retention.ms", "-1",
            "size", String.valueOf(CHUNK_SIZE),
            "chunk.size", String.valueOf(CHUNK_SIZE),
            "thread.pool.size", "1"
        );
    }
}
