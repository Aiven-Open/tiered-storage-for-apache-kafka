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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

/**
 * Tests metrics gathering on Chunk Cache implementations
 */
@ExtendWith(MockitoExtension.class)
class AbstractChunkCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final ChunkKey CHUNK_KEY = new ChunkKey("topic/segment", 0);

    @TempDir
    static Path baseCachePath;

    private static Stream<Arguments> caches() {
        return Stream.of(
            Arguments.of(
                DiskBasedChunkCache.class,
                Map.of(
                    "retention.ms", "-1",
                    "size", "-1",
                    "path", baseCachePath.toString()
                )
            ),
            Arguments.of(
                InMemoryChunkCache.class,
                Map.of(
                    "retention.ms", "-1",
                    "size", "-1"
                )
            ));
    }

    @ParameterizedTest(name = "Cache {0}")
    @MethodSource("caches")
    void shouldRecordMetrics(final Class<AbstractChunkCache<?>> chunkCacheClass, final Map<String, ?> config)
        throws Exception {
        final var chunkCache = chunkCacheClass.getDeclaredConstructor().newInstance();
        chunkCache.configure(config);


        final var objectName = new ObjectName("aiven.kafka.server.tieredstorage.cache:type=chunk-cache");

        // When getting a existing chunk from cache
        chunkCache.getChunk(CHUNK_KEY, this::testChunkSupplier);

        // check cache size increases after first miss
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-size-total"))
            .isEqualTo(1.0);

        chunkCache.getChunk(CHUNK_KEY, this::testChunkSupplier);

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
    }

    private CompletableFuture<InputStream> testChunkSupplier() {
        return CompletableFuture.completedFuture(
            new ByteArrayInputStream("test".getBytes())
        );
    }
}
