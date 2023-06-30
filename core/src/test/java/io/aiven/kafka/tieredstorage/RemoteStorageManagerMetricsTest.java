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
import java.io.InputStream;
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

import io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RemoteStorageManagerMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final long METRIC_TIME_WINDOW_SEC =
        TimeUnit.SECONDS.convert(new MetricConfig().timeWindowMs(), TimeUnit.MILLISECONDS);

    static RemoteStorageManager rsm;

    static final int LOG_SEGMENT_BYTES = 10;
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                Uuid.randomUuid()),
            1, -1, -1, -1, 1L,
            LOG_SEGMENT_BYTES, Collections.singletonMap(1, 100L));
    static LogSegmentData logSegmentData;

    @BeforeEach
    void setup(@TempDir final Path tmpDir,
                      @Mock final Time time) throws IOException {
        when(time.milliseconds()).thenReturn(0L);
        rsm = new RemoteStorageManager(time);

        final Path target = tmpDir.resolve("target");
        Files.createDirectories(target);

        rsm.configure(Map.of(
            "chunk.size", "123",
            "storage.backend.class",
            "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", target.toString(),
            "chunk.cache.path", tmpDir.resolve("cache").toString(),
            "chunk.cache.class", InMemoryChunkCache.class.getCanonicalName(),
            "chunk.cache.size", 100 * 1024 * 1024
        ));

        final Path source = tmpDir.resolve("source");
        Files.createDirectories(source);
        final Path sourceFile = source.resolve("file");
        Files.write(sourceFile, new byte[LOG_SEGMENT_BYTES]);

        logSegmentData = new LogSegmentData(
            sourceFile, sourceFile, sourceFile, Optional.empty(), sourceFile,
            ByteBuffer.allocate(0)
        );
    }

    @Test
    void metricsShouldBeReported() throws RemoteStorageException, JMException {
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);

        rsm.fetchLogSegment(REMOTE_LOG_SEGMENT_METADATA, 0);

        final ObjectName rsmMetricsName = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics");
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-rate"))
            .isEqualTo(3.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-total"))
            .isEqualTo(3.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-bytes-rate"))
            .isEqualTo(30.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-bytes-total"))
            .isEqualTo(30.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-time-avg"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-time-max"))
            .isEqualTo(0.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-fetch-rate"))
            .isEqualTo(1.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-fetch-total"))
            .isEqualTo(1.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-fetch-requested-bytes-rate"))
            .isEqualTo(10.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-fetch-requested-bytes-total"))
            .isEqualTo(10.0);

        rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);
        rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-rate"))
            .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-bytes-rate"))
            .isEqualTo(20.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-bytes-total"))
            .isEqualTo(20.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-time-avg"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-delete-time-max"))
            .isEqualTo(0.0);
    }

    @Test
    void metricsErrorsShouldBeReported() throws JMException {
        final var failingStorage = new StorageBackend() {
            @Override
            public void delete(final String key) throws StorageBackendException {
                throw new StorageBackendException("something wrong");
            }

            @Override
            public InputStream fetch(final String key) throws StorageBackendException {
                return null;
            }

            @Override
            public InputStream fetch(final String key, final BytesRange range) throws StorageBackendException {
                return null;
            }

            @Override
            public void upload(final InputStream inputStream, final String key) throws StorageBackendException {
                throw new StorageBackendException("something wrong");
            }

            @Override
            public void configure(final Map<String, ?> configs) {
            }
        };

        rsm.setStorage(failingStorage);

        try {
            rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        } catch (final Exception e) {
            // let it fail
        }
        try {
            rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        } catch (final Exception e) {
            // let it fail
        }
        try {
            rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        } catch (final Exception e) {
            // let it fail
        }

        final ObjectName rsmMetricsName = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics");
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-rate"))
            .isEqualTo(3.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-total"))
            .isEqualTo(3.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-bytes-rate"))
            .isEqualTo(30.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-bytes-total"))
            .isEqualTo(30.0);

        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-errors-rate"))
            .isEqualTo(3.0 / METRIC_TIME_WINDOW_SEC);
        assertThat(MBEAN_SERVER.getAttribute(rsmMetricsName, "segment-copy-errors-total"))
            .isEqualTo(3.0);

        try {
            rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);
        } catch (final Exception e) {
            // let it fail
        }
        try {
            rsm.deleteLogSegmentData(REMOTE_LOG_SEGMENT_METADATA);
        } catch (final Exception e) {
            // let it fail
        }

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
