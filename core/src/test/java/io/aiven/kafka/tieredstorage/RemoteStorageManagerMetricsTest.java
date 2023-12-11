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

package io.aiven.kafka.tieredstorage;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RemoteStorageManagerMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final long METRIC_TIME_WINDOW_SEC =
        TimeUnit.SECONDS.convert(new MetricConfig().timeWindowMs(), TimeUnit.MILLISECONDS);
    static final int LOG_SEGMENT_BYTES = 10;
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                Uuid.randomUuid()),
            1, 100, -1, -1, 1L,
            LOG_SEGMENT_BYTES, Collections.singletonMap(1, 100L));

    RemoteStorageManager rsm;
    LogSegmentData logSegmentData;

    private Map<String, Object> configs;

    @BeforeEach
    void setup(@TempDir final Path tmpDir,
               @Mock final Time time) throws IOException {
        when(time.milliseconds()).thenReturn(0L);
        rsm = new RemoteStorageManager(time);

        final Path target = tmpDir.resolve("target");
        Files.createDirectories(target);

        configs = Map.of(
            "chunk.size", "123",
            "storage.backend.class",
            "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", target.toString(),
            "fetch.chunk.cache.path", tmpDir.resolve("cache").toString(),
            "fetch.chunk.cache.class", MemoryChunkCache.class.getCanonicalName(),
            "fetch.chunk.cache.size", 100 * 1024 * 1024,
            "metrics.recording.level", "DEBUG"
        );

        final Path source = tmpDir.resolve("source");
        Files.createDirectories(source);
        final Path sourceFile = source.resolve("file");
        Files.write(sourceFile, new byte[LOG_SEGMENT_BYTES]);

        final var leaderEpoch = ByteBuffer.wrap(new byte[LOG_SEGMENT_BYTES]);
        logSegmentData = new LogSegmentData(
            sourceFile, sourceFile, sourceFile, Optional.empty(), sourceFile,
            leaderEpoch
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ",topic=topic", ",topic=topic,partition=0"})
    void metricsShouldBeReported(final String tags) throws RemoteStorageException, JMException {
        rsm.configure(configs);

        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        logSegmentData.leaderEpochIndex().flip(); // so leader epoch can be consumed again
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        logSegmentData.leaderEpochIndex().flip();
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        logSegmentData.leaderEpochIndex().flip();

        final var objectName = "aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics" + tags;
        final ObjectName metricName = ObjectName.getInstance(objectName);

        // upload related metrics
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-copy-time-avg"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-copy-time-max"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(metricName, "object-upload-total"))
            .isEqualTo(9.0);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "object-upload-rate"))
            .isEqualTo(9.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(metricName, "object-upload-bytes-total"))
            .isEqualTo(2160.0);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "object-upload-bytes-rate"))
            .isEqualTo(2160.0 / METRIC_TIME_WINDOW_SEC);

        for (final var suffix : ObjectKeyFactory.Suffix.values()) {
            final ObjectName storageMetricsName = ObjectName.getInstance(objectName + ",object-type=" + suffix.value);
            assertThat(MBEAN_SERVER.getAttribute(storageMetricsName, "object-upload-rate"))
                .isEqualTo(3.0 / METRIC_TIME_WINDOW_SEC);
            assertThat(MBEAN_SERVER.getAttribute(storageMetricsName, "object-upload-total"))
                .isEqualTo(3.0);
            assertThat(MBEAN_SERVER.getAttribute(storageMetricsName, "object-upload-bytes-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
            assertThat(MBEAN_SERVER.getAttribute(storageMetricsName, "object-upload-bytes-total"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }

        // fetch related metrics
        final var segmentManifestCacheObjectName =
            new ObjectName("aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics");

        rsm.fetchLogSegment(REMOTE_LOG_SEGMENT_METADATA, 0);

        // check cache size increases after first miss
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-size-total"))
            .isEqualTo(1.0);

        rsm.fetchLogSegment(REMOTE_LOG_SEGMENT_METADATA, 0);

        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-fetch-requested-bytes-rate"))
            .isEqualTo(20.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-fetch-requested-bytes-total"))
            .isEqualTo(20.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-size-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-hits-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-misses-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-success-time-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0);

        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-success-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-failure-time-total"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-failure-total"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-weight-total"))
            .isEqualTo(0.0);

        rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);
        rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);

        // delete related metrics
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-rate"))
            .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-bytes-rate"))
            .isEqualTo(20.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-bytes-total"))
            .isEqualTo(20.0);

        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-time-avg"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(metricName, "segment-delete-time-max"))
            .isEqualTo(0.0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ",topic=topic", ",topic=topic,partition=0"})
    void metricsErrorsShouldBeReported(final String tags) throws JMException {
        final var testException = new StorageBackendException("something wrong");
        try (@SuppressWarnings("unused") final var storage = mockConstruction(
            FileSystemStorage.class,
            (mock, context) -> {
                doThrow(testException).when(mock).upload(any(), any());
                doThrow(testException).when(mock).delete(any());
            }
        )) {

            rsm.configure(configs);

            final ObjectName rsmMetricsName = ObjectName.getInstance(
                "aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics" + tags);

            // checking that deletion fails with expected exceptions
            assertThatThrownBy(() -> rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA))
                .isInstanceOf(RemoteStorageException.class)
                .hasRootCause(testException);
            assertThatThrownBy(() -> rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA))
                .isInstanceOf(RemoteStorageException.class)
                .hasRootCause(testException);

            // verifying deletion failure metrics
            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-rate"))
                .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);
            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-total"))
                .isEqualTo(2.0);

            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-bytes-rate"))
                .isEqualTo(20.0 / METRIC_TIME_WINDOW_SEC);
            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-bytes-total"))
                .isEqualTo(20.0);

            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-errors-rate"))
                .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);
            assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-errors-total"))
                .isEqualTo(2.0);
        }
    }
}
