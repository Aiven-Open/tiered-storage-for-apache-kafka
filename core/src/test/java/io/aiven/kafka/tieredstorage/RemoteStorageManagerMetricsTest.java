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

import org.junit.jupiter.api.BeforeAll;
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

    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                Uuid.randomUuid()),
            1, -1, -1, -1, 1L,
            1, Collections.singletonMap(1, 100L));
    static LogSegmentData logSegmentData;

    @BeforeAll
    static void setup(@TempDir final Path tmpDir,
                      @Mock final Time time) throws IOException {
        when(time.milliseconds()).thenReturn(0L);
        rsm = new RemoteStorageManager(time);

        final Path target = tmpDir.resolve("target");
        Files.createDirectories(target);

        rsm.configure(Map.of(
            "chunk.size", "123",
            "storage.backend.class",
            "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", target.toString()
        ));

        final Path source = tmpDir.resolve("source");
        Files.createDirectories(source);
        final Path sourceFile = source.resolve("file");
        Files.write(sourceFile, new byte[10]);

        logSegmentData = new LogSegmentData(
            sourceFile, sourceFile, sourceFile, Optional.empty(), sourceFile,
            ByteBuffer.allocate(0)
        );
    }

    @Test
    void metricsShouldBeReported() throws RemoteStorageException, JMException, IOException {
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);
        rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData);

        final InputStream resultInputStream = rsm.fetchLogSegment(REMOTE_LOG_SEGMENT_METADATA, 0);

        final ObjectName segmentCopyPerSecName = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics");
        assertThat((double) MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "segment-copy-rate"))
            .isEqualTo(3.0 / METRIC_TIME_WINDOW_SEC);

        assertThat((double) MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "segment-copy-time-avg"))
            .isZero();
        assertThat((double) MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "segment-copy-time-max"))
            .isZero();

        assertThat((double) MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "segment-fetch-rate"))
            .isEqualTo(1.0 / METRIC_TIME_WINDOW_SEC);
    }
}
