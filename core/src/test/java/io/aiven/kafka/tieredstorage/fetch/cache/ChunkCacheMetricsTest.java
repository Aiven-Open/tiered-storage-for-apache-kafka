/*
 * Copyright 2023 Aiven Oy
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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * Tests metrics gathering on Chunk Cache implementations
 */
@ExtendWith(MockitoExtension.class)
class ChunkCacheMetricsTest {
    private static final ChunkIndex FIXED_SIZE_CHUNK_INDEX = new FixedSizeChunkIndex(10, 10, 10, 10);
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    public static final ObjectKey OBJECT_KEY_PATH = () -> "topic/segment";

    @TempDir
    static Path baseCachePath;

    @Mock
    ChunkManager chunkManager;
    @Mock
    SegmentManifest segmentManifest;

    private static Stream<Arguments> caches() {
        return Stream.of(
            Arguments.of(
                DiskChunkCache.class,
                Map.of(
                    "retention.ms", "-1",
                    "size", "-1",
                    "path", baseCachePath.toString(),
                    "thread.pool.size", 4
                )
            ),
            Arguments.of(
                MemoryChunkCache.class,
                Map.of(
                    "retention.ms", "-1",
                    "size", "-1",
                    "thread.pool.size", 4
                )
            ));
    }

    @BeforeEach
    void setUp() {
        when(segmentManifest.chunkIndex()).thenReturn(FIXED_SIZE_CHUNK_INDEX);
    }

    @ParameterizedTest(name = "Cache {0}")
    @MethodSource("caches")
    void shouldRecordMetrics(final Class<ChunkCache<?>> chunkCacheClass, final Map<String, ?> config)
        throws Exception {
        // Given a fetch chunk cache implementation
        when(chunkManager.getChunk(any(), any(), anyInt()))
            .thenReturn(new ByteArrayInputStream("test".getBytes()));

        final var chunkCache = chunkCacheClass.getDeclaredConstructor(ChunkManager.class).newInstance(chunkManager);
        chunkCache.configure(config);

        final var objectName = new ObjectName("aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics");

        // When getting a existing chunk from cache
        chunkCache.getChunk(OBJECT_KEY_PATH, segmentManifest, 0);

        // check cache size increases after first miss
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-size-total"))
            .isEqualTo(1.0);

        chunkCache.getChunk(OBJECT_KEY_PATH, segmentManifest, 0);

        // Then the following metrics should be available
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-hits-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-misses-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-success-time-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0);

        // compute is considered as load success regardless if present or not
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-success-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-failure-time-total"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-failure-total"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-eviction-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-eviction-weight-total"))
            .isEqualTo(0.0);

        final var threadPoolObjectName =
            new ObjectName("aiven.kafka.server.tieredstorage.thread-pool:type=chunk-cache-thread-pool-metrics");

        // The following assertions are relaxed, just to show that metrics are collected
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "parallelism-total"))
            .isEqualTo(4.0);
        // approximation to completed tasks
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "steal-task-count-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThanOrEqualTo(0.0);
        // wait for thread-pool to drain queued tasks
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() ->
                assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "queued-task-count-total"))
                    .isEqualTo(0.0));
    }
}
