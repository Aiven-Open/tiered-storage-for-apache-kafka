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

package io.aiven.kafka.tieredstorage.fetch.index;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
class MemorySegmentIndexesCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    public static final ObjectKey OBJECT_KEY_PATH = () -> "topic/segment";
    public static final Supplier<byte[]> INDEX_SUPPLIER = () -> "test".getBytes(StandardCharsets.UTF_8);

    @Test
    void shouldRecordMetrics()
        throws Exception {
        // Given
        final var cache = new MemorySegmentIndexesCache();
        cache.configure(Map.of(
            "size", "-1",
            "retention.ms", "100",
            "thread.pool.size", "4"
        ));

        final var objectName =
            new ObjectName("aiven.kafka.server.tieredstorage.cache:type=segment-indexes-cache-metrics");

        // When getting a existing chunk from cache
        cache.get(OBJECT_KEY_PATH, RemoteStorageManager.IndexType.OFFSET, INDEX_SUPPLIER);

        // check cache size increases after first miss
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-size-total"))
            .isEqualTo(1.0);

        cache.get(OBJECT_KEY_PATH, RemoteStorageManager.IndexType.OFFSET, INDEX_SUPPLIER);

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
            new ObjectName("aiven.kafka.server.tieredstorage.thread-pool:type="
                + "segment-indexes-cache-thread-pool-metrics");

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
