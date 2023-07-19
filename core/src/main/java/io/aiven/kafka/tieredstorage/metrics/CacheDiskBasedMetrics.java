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

package io.aiven.kafka.tieredstorage.metrics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCache;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheDiskBasedMetrics {
    private static final Logger log = LoggerFactory.getLogger(DiskBasedChunkCache.class);

    final Sensor pathSizeSensor;
    final Sensor pathWrittenSensor;
    final Sensor pathDeletedSensor;

    AtomicLong pathSize;

    public CacheDiskBasedMetrics(final String groupName, final Path directory) {
        final JmxReporter reporter = new JmxReporter();

        final org.apache.kafka.common.metrics.Metrics metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.cache.disk")
        );

        pathSizeSensor = new SensorProvider(metrics, "path-size")
            .with(new MetricNameTemplate("path-size", groupName, ""), new Value())
            .get();
        final MetricNameTemplate pathWriteRate = new MetricNameTemplate("path-write-rate", groupName, "");
        final MetricNameTemplate pathWriteTotal = new MetricNameTemplate("path-write-rate", groupName, "");
        pathWrittenSensor = new SensorProvider(metrics, "path-write")
            .with(pathWriteRate, new Rate())
            .with(pathWriteTotal, new CumulativeSum())
            .get();
        final MetricNameTemplate pathDeleteRate = new MetricNameTemplate("path-delete-rate", groupName, "");
        final MetricNameTemplate pathDeleteTotal = new MetricNameTemplate("path-delete-rate", groupName, "");
        pathDeletedSensor = new SensorProvider(metrics, "path-delete")
            .with(pathDeleteRate, new Rate())
            .with(pathDeleteTotal, new CumulativeSum())
            .get();

        pathSize = new AtomicLong(FileUtils.sizeOfDirectory(directory.toFile()));
    }

    public void recordPathDeleted(final Path path) {
        try {
            final var size = Files.size(path);
            final var dirSize = pathSize.addAndGet(-size);
            pathDeletedSensor.record(size);
            pathSizeSensor.record(dirSize);
        } catch (final IOException e) {
            log.error("Error when recording path [{}] size", path, e);
        }
    }

    public void recordPathWritten(final Path path) {
        try {
            final var size = Files.size(path);
            final var dirSize = pathSize.addAndGet(size);
            pathWrittenSensor.record(size);
            pathSizeSensor.record(dirSize);
        } catch (final IOException e) {
            log.error("Error when recording path [{}] size", path, e);
        }
    }
}
