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
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;

import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DiskBasedChunkCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final long METRIC_TIME_WINDOW_SEC =
        TimeUnit.SECONDS.convert(new MetricConfig().timeWindowMs(), TimeUnit.MILLISECONDS);

    public static final Uuid SEGMENT_ID = Uuid.randomUuid();

    static final int LOG_SEGMENT_BYTES = 10;
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                SEGMENT_ID),
            1, -1, -1, -1, 1L,
            LOG_SEGMENT_BYTES, Collections.singletonMap(1, 100L));

    @Mock
    ChunkManager chunkManager;
    @Mock
    SegmentManifest segmentManifest;
    @TempDir
    Path baseCachePath;

    DiskBasedChunkCache diskBasedChunkCache;

    @BeforeEach
    void setUp() {
        diskBasedChunkCache = new DiskBasedChunkCache(chunkManager);
        diskBasedChunkCache.configure(Map.of(
            "retention.ms", "-1",
            "size", "-1",
            "path", baseCachePath.toString()
        ));
    }

    @Test
    void cacheChunks() throws Exception {
        when(chunkManager.getChunk(any(), eq(segmentManifest), eq(0)))
            .thenReturn(new ByteArrayInputStream("test".getBytes()));

        diskBasedChunkCache.getChunk(REMOTE_LOG_SEGMENT_METADATA, segmentManifest, 0);
        diskBasedChunkCache.getChunk(REMOTE_LOG_SEGMENT_METADATA, segmentManifest, 0);

        final var segmentManifestCacheObjectName =
            new ObjectName("aiven.kafka.server.tieredstorage.cache:type=chunk-cache");
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-hits-total"))
            .isEqualTo(1.0);
        AssertionsForClassTypes.assertThat(
                (double) MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-hits-rate"))
            .isCloseTo(1.0 / METRIC_TIME_WINDOW_SEC, Percentage.withPercentage(99));
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-misses-total"))
            .isEqualTo(1.0);
        AssertionsForClassTypes.assertThat(
                (double) MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-misses-rate"))
            .isCloseTo(1.0 / METRIC_TIME_WINDOW_SEC, Percentage.withPercentage(99));
        AssertionsForClassTypes.assertThat(
                (double) MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-success-time-total"))
            .isGreaterThan(0);

        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-success-total"))
            .isEqualTo(1.0);
        AssertionsForClassTypes.assertThat(
                (double) MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-success-rate"))
            .isCloseTo(1.0 / METRIC_TIME_WINDOW_SEC, Percentage.withPercentage(99));
        AssertionsForClassTypes.assertThat(
                (double) MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-failure-time-total"))
            .isEqualTo(0);

        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-failure-total"))
            .isEqualTo(0.0);
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-load-failure-rate"))
            .isEqualTo(0.0);

        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-total"))
            .isEqualTo(0.0);
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-rate"))
            .isEqualTo(0.0);
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-weight-total"))
            .isEqualTo(0.0);
        AssertionsForClassTypes.assertThat(
                MBEAN_SERVER.getAttribute(segmentManifestCacheObjectName, "cache-eviction-weight-rate"))
            .isEqualTo(0.0);
    }
}
