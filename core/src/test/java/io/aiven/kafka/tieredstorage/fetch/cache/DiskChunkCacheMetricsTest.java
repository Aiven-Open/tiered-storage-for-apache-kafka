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

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.fetch.DefaultChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DiskChunkCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final long METRIC_TIME_WINDOW_SEC =
        TimeUnit.SECONDS.convert(new MetricConfig().timeWindowMs(), TimeUnit.MILLISECONDS);

    static final SegmentManifest SEGMENT_MANIFEST =
        new SegmentManifestV1(
            new FixedSizeChunkIndex(10, 30, 10, 10),
            SegmentIndexesV1.builder()
                .add(RemoteStorageManager.IndexType.OFFSET, 1)
                .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
                .add(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, 1)
                .add(RemoteStorageManager.IndexType.LEADER_EPOCH, 1)
                .add(RemoteStorageManager.IndexType.TRANSACTION, 1)
                .build(),
            false, null, null);

    static final ObjectKey OBJECT_KEY_PATH = () -> "topic/segment";

    @TempDir
    Path baseCachePath;

    @Test
    void metrics() throws IOException, JMException, StorageBackendException {
        final Time time = mock(Time.class);
        when(time.milliseconds()).thenReturn(0L);

        final int size1 = 1024;
        final int size2 = 10;

        final DefaultChunkManager chunkManager = mock(DefaultChunkManager.class);
        when(chunkManager.getChunk(OBJECT_KEY_PATH, SEGMENT_MANIFEST, 0))
            .thenReturn(new ByteArrayInputStream(new byte[size1]));
        when(chunkManager.getChunk(OBJECT_KEY_PATH, SEGMENT_MANIFEST, 1))
            .thenReturn(new ByteArrayInputStream(new byte[size2]));

        final DiskChunkCache diskChunkCache = new DiskChunkCache(chunkManager, time);
        diskChunkCache.configure(Map.of(
            "size", size1,  // enough to put the first, but not both
            "path", baseCachePath.toString()
        ));

        diskChunkCache.getChunk(OBJECT_KEY_PATH, SEGMENT_MANIFEST, 0);

        final var objectName = new ObjectName("aiven.kafka.server.tieredstorage.cache:type=chunk-cache-disk");

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-rate"))
            .isEqualTo(1.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-total"))
            .isEqualTo((double) size1);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-rate"))
            .isEqualTo(((double) size1) / METRIC_TIME_WINDOW_SEC);

        diskChunkCache.getChunk(OBJECT_KEY_PATH, SEGMENT_MANIFEST, 1);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-rate"))
            .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-total"))
            .isEqualTo((double) (size1 + size2));
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-rate"))
            .isEqualTo(((double) (size1 + size2)) / METRIC_TIME_WINDOW_SEC);

        await("Deletion happens").atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
            .until(() -> (double) MBEAN_SERVER.getAttribute(objectName, "delete-total") > 0);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "delete-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "delete-rate"))
            .isEqualTo(1.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "delete-bytes-total"))
            .isEqualTo((double) size1);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "delete-bytes-rate"))
            .isEqualTo(((double) size1) / METRIC_TIME_WINDOW_SEC);
    }
}
