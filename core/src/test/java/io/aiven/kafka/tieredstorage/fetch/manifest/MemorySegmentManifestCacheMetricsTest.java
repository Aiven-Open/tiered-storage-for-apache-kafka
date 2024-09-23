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

package io.aiven.kafka.tieredstorage.fetch.manifest;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
class MemorySegmentManifestCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    public static final ObjectKey OBJECT_KEY_PATH = () -> "topic/segment";

    static final String MANIFEST =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"segmentIndexes\":{"
            + "\"offset\":{\"position\":0,\"size\":1},"
            + "\"timestamp\":{\"position\":1,\"size\":1},"
            + "\"producerSnapshot\":{\"position\":2,\"size\":1},"
            + "\"leaderEpoch\":{\"position\":3,\"size\":1},"
            + "\"transaction\":{\"position\":4,\"size\":1}"
            + "},"
            + "\"compression\":false}";
    public static final ObjectFetcher FILE_FETCHER = new ObjectFetcher() {
        @Override
        public InputStream fetch(final ObjectKey key) {
            return new ByteArrayInputStream(MANIFEST.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public InputStream fetch(final ObjectKey key, final BytesRange range) {
            return new ByteArrayInputStream(MANIFEST.getBytes(StandardCharsets.UTF_8));
        }
    };

    static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new Jdk8Module());
        MAPPER.registerModule(KafkaTypeSerdeModule.create());
    }

    @Test
    void shouldRecordMetrics()
        throws Exception {
        // Given
        final var cache = new MemorySegmentManifestCache(FILE_FETCHER, MAPPER);
        cache.configure(Map.of(
            "size", "-1",
            "retention.ms", "10000",
            "thread.pool.size", "4"
        ));

        final var objectName =
            new ObjectName("aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics");

        // When getting a existing chunk from cache
        cache.get(OBJECT_KEY_PATH);

        // check cache size increases after first miss
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-size-total"))
            .isEqualTo(1.0);

        cache.get(OBJECT_KEY_PATH);

        // Then the following metrics should be available
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-hits-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-misses-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-success-time-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0);

        // when using cache loader, load success seem to be only the missed loads (hits do not count)
        assertThat(MBEAN_SERVER.getAttribute(objectName, "cache-load-success-total"))
            .isEqualTo(1.0);
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
                + "segment-manifest-cache-thread-pool-metrics");

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
