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

import java.util.List;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

class DiskBasedChunkCacheMetrics {
    private static final String METRIC_GROUP = "chunk-cache-disk";

    private final Metrics metrics;

    private final Sensor writes;
    private final Sensor writeBytes;
    private final Sensor deletes;
    private final Sensor deleteBytes;

    DiskBasedChunkCacheMetrics(final Time time) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.cache")
        );

        writes = createSensor("write");
        writeBytes = createSensor("write-bytes");
        deletes = createSensor("delete");
        deleteBytes = createSensor("delete-bytes");
    }

    private Sensor createSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-rate", METRIC_GROUP), new Rate());
        sensor.add(metrics.metricName(name + "-total", METRIC_GROUP), new CumulativeSum());
        return sensor;
    }

    void chunkWritten(final long bytesWritten) {
        this.writes.record(1);
        this.writeBytes.record(bytesWritten);
    }

    void chunkDeleted(final long chunkSize) {
        this.deletes.record(1);
        this.deleteBytes.record(chunkSize);
    }
}
