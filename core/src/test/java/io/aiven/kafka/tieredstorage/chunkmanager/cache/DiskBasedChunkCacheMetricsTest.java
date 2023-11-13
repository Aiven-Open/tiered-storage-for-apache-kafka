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

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.DefaultChunkManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DiskBasedChunkCacheMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    static final long METRIC_TIME_WINDOW_SEC =
        TimeUnit.SECONDS.convert(new MetricConfig().timeWindowMs(), TimeUnit.MILLISECONDS);

    @TempDir
    Path baseCachePath;

    @Test
    void metrics() throws IOException, JMException {
        final Time time = mock(Time.class);
        when(time.milliseconds()).thenReturn(0L);
        final DiskBasedChunkCache diskBasedChunkCache = new DiskBasedChunkCache(mock(DefaultChunkManager.class), time);
        diskBasedChunkCache.configure(Map.of(
            "size", 1000000,
            "path", baseCachePath.toString()
        ));

        final int size1 = 1024;
        final var chunkKey1 = new ChunkKey("topic/segment", 0);
        diskBasedChunkCache.cacheChunk(chunkKey1, new ByteArrayInputStream(new byte[size1]));

        final var objectName = new ObjectName("aiven.kafka.server.tieredstorage.cache:type=chunk-cache-disk");

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-rate"))
            .isEqualTo(1.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-total"))
            .isEqualTo((double) size1);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-rate"))
            .isEqualTo(((double) size1) / METRIC_TIME_WINDOW_SEC);

        final int size2 = 10;
        final var chunkKey2 = new ChunkKey("topic/segment", 1);
        diskBasedChunkCache.cacheChunk(chunkKey2, new ByteArrayInputStream(new byte[size2]));

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-rate"))
            .isEqualTo(2.0 / METRIC_TIME_WINDOW_SEC);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-total"))
            .isEqualTo((double) (size1 + size2));
        assertThat(MBEAN_SERVER.getAttribute(objectName, "write-bytes-rate"))
            .isEqualTo(((double) (size1 + size2)) / METRIC_TIME_WINDOW_SEC);
    }
}
