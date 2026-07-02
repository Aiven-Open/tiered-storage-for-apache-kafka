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

package io.aiven.kafka.tieredstorage.e2e;

import java.io.IOException;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.github.dockerjava.api.model.Ports;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test using DirectMemoryChunkCache with FileSystem backend.
 * Validates all standard tiered storage scenarios plus direct memory leak detection.
 */
public class DirectMemoryCacheLocalSystemTest extends LocalSystemSingleBrokerTest {
    static final long CACHE_SIZE = 16L * 1024 * 1024; // 16MB
    static final int JMX_PORT = 9999;

    @BeforeAll
    static void init() throws Exception {
        setupKafka(kafka -> {
            tieredDataDir = baseDir.resolve(TS_DATA_SUBDIR_HOST);
            tieredDataDir.toFile().mkdirs();
            tieredDataDir.toFile().setWritable(true, false);

            kafka
                .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                    "/tiered-storage-for-apache-kafka/core/*")
                .withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                    "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage")
                .withEnv("KAFKA_RSM_CONFIG_STORAGE_ROOT", TS_DATA_DIR_CONTAINER)
                .withFileSystemBind(tieredDataDir.toString(), TS_DATA_DIR_CONTAINER)
                // DirectMemoryChunkCache configuration
                .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_CLASS",
                    "io.aiven.kafka.tieredstorage.fetch.cache.DirectMemoryChunkCache")
                .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_SIZE",
                    Long.toString(CACHE_SIZE))
                .withEnv("KAFKA_RSM_CONFIG_FETCH_CHUNK_CACHE_CHUNK_SIZE",
                    Integer.toString(CHUNK_SIZE))
                .withEnv("KAFKA_OPTS", "-Djava.rmi.server.hostname=localhost")
                // Enable JMX remote access
                .withEnv("KAFKA_JMX_PORT", Integer.toString(JMX_PORT))
                .withEnv("KAFKA_JMX_HOSTNAME", "localhost");
            kafka.addExposedPort(JMX_PORT);
            kafka.withCreateContainerCmdModifier(cmd -> {
                final Ports ports = cmd.getHostConfig().getPortBindings();
                ports.bind(new com.github.dockerjava.api.model.ExposedPort(JMX_PORT),
                    Ports.Binding.bindPort(JMX_PORT));
                cmd.getHostConfig().withPortBindings(ports);
            });
        });
    }

    @Test
    @Order(6)
    void verifyPoolMetricsAndNoLeak() throws Exception {
        // After all topics are deleted (Order 5), wait for async eviction.
        Thread.sleep(3000);

        final String pid = getKafkaPid();
        triggerGC(pid);

        try (JMXConnector connector = connectJmx()) {
            final MBeanServerConnection mbs = connector.getMBeanServerConnection();

            // --- DirectMemoryChunkCacheMetrics (buffer pool) ---
            final ObjectName poolObj = new ObjectName(
                "aiven.kafka.server.tieredstorage.cache:type=direct-memory-chunk-cache-metrics");

            final long activeCount = ((Number) mbs.getAttribute(poolObj, "buffer-pool-active-count")).longValue();
            final long poolSize = ((Number) mbs.getAttribute(poolObj, "buffer-pool-size")).longValue();
            final long maxSize = ((Number) mbs.getAttribute(poolObj, "buffer-pool-max-size")).longValue();
            final long allocatedBytes =
                ((Number) mbs.getAttribute(poolObj, "buffer-pool-allocated-bytes")).longValue();
            final double hitTotal = ((Number) mbs.getAttribute(poolObj, "buffer-pool-hit-total")).doubleValue();
            final double missTotal = ((Number) mbs.getAttribute(poolObj, "buffer-pool-miss-total")).doubleValue();
            final double hitRate = ((Number) mbs.getAttribute(poolObj, "buffer-pool-hit-rate")).doubleValue();
            final double missRate = ((Number) mbs.getAttribute(poolObj, "buffer-pool-miss-rate")).doubleValue();
            final double acquireBytesTotal =
                ((Number) mbs.getAttribute(poolObj, "buffer-pool-acquire-bytes-total")).doubleValue();
            final double acquireBytesRate =
                ((Number) mbs.getAttribute(poolObj, "buffer-pool-acquire-bytes-rate")).doubleValue();

            LOG.info("Pool Gauges: activeCount={}, poolSize={}, maxSize={}, allocatedBytes={}",
                activeCount, poolSize, maxSize, allocatedBytes);
            LOG.info("Pool Sensors: hitTotal={}, missTotal={}, acquireBytesTotal={}, hitRate={}, missRate={}, "
                    + "acquireBytesRate={}", hitTotal, missTotal, acquireBytesTotal, hitRate, missRate,
                acquireBytesRate);

            // --- CaffeineStatsCounter (cache-level) ---
            final ObjectName cacheObj = new ObjectName(
                "aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics");
            final double cacheSize = ((Number) mbs.getAttribute(cacheObj, "cache-size-total")).doubleValue();
            LOG.info("Cache metrics: cacheSize={}", cacheSize);

            // Leak detection: all active buffers must be accounted for by cache entries
            assertThat(activeCount)
                .as("Active buffers should equal cache size (no leak)")
                .isEqualTo((long) cacheSize);

            // Pool invariants
            assertThat(poolSize).as("poolSize <= maxSize").isLessThanOrEqualTo(maxSize);
            assertThat(allocatedBytes)
                .as("Total allocated bytes bounded")
                .isLessThanOrEqualTo(CACHE_SIZE + 8L * 1024 * 1024);

            // Sensors recorded activity during the test
            assertThat(hitTotal + missTotal).as("At least one acquire").isGreaterThan(0);
            assertThat(hitTotal / (hitTotal + missTotal))
                .as("Pool reuse ratio should be positive")
                .isGreaterThan(0);
            assertThat(acquireBytesTotal).as("Acquire bytes positive").isGreaterThan(0);
            assertThat(hitRate).as("Hit rate non-negative").isGreaterThanOrEqualTo(0);
            assertThat(missRate).as("Miss rate non-negative").isGreaterThanOrEqualTo(0);
            assertThat(acquireBytesRate).as("Acquire bytes rate non-negative").isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @Order(7)
    void jvmDirectMemoryBounded() throws Exception {
        try (JMXConnector connector = connectJmx()) {
            final MBeanServerConnection mbs = connector.getMBeanServerConnection();
            final ObjectName directPoolName = new ObjectName("java.nio:type=BufferPool,name=direct");
            final long directMemoryUsed = (Long) mbs.getAttribute(directPoolName, "MemoryUsed");
            LOG.info("BufferPoolMXBean: memoryUsed={} bytes", directMemoryUsed);

            assertThat(directMemoryUsed)
                .as("JVM direct memory bounded by pool capacity + Kafka overhead")
                .isLessThanOrEqualTo(CACHE_SIZE + 8L * 1024 * 1024);
        }
    }

    private String getKafkaPid() throws Exception {
        final var pidResult = kafka.execInContainer("bash", "-c", "pgrep -f kafka.Kafka || echo 1");
        return pidResult.getStdout().trim().lines().findFirst().orElse("1");
    }

    private void triggerGC(final String pid) throws Exception {
        kafka.execInContainer("bash", "-c", "jcmd " + pid + " GC.run 2>&1");
        Thread.sleep(2000);
    }

    private JMXConnector connectJmx() throws IOException {
        final String url = String.format(
            "service:jmx:rmi:///jndi/rmi://localhost:%d/jmxrmi", JMX_PORT);
        return JMXConnectorFactory.connect(new JMXServiceURL(url), null);
    }
}
