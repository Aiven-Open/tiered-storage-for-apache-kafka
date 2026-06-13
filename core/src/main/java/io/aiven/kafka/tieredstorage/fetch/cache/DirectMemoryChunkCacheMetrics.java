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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.Closeable;
import java.util.List;

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

class DirectMemoryChunkCacheMetrics implements Closeable {
    private static final String METRIC_GROUP = "direct-memory-chunk-cache-metrics";
    private static final String METRIC_CONTEXT = "aiven.kafka.server.tieredstorage.cache";

    private final Metrics metrics;
    private final Sensor poolHits;
    private final Sensor poolMisses;
    private final Sensor acquireBytes;

    DirectMemoryChunkCacheMetrics(final DirectByteBufferPool bufferPool) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        poolHits = createSensor("buffer-pool-hit");
        poolMisses = createSensor("buffer-pool-miss");
        acquireBytes = createSensor("buffer-pool-acquire-bytes");

        metrics.addMetric(
            metrics.metricName("buffer-pool-size", METRIC_GROUP, "Number of buffers currently in pool"),
            (Gauge<Long>) (config, now) -> bufferPool.currentPoolSize()
        );
        metrics.addMetric(
            metrics.metricName("buffer-pool-max-size", METRIC_GROUP, "Maximum pool capacity"),
            (Gauge<Long>) (config, now) -> bufferPool.maxPoolSize()
        );
        metrics.addMetric(
            metrics.metricName("buffer-pool-active-count", METRIC_GROUP,
                "Number of buffers currently checked out from pool"),
            (Gauge<Long>) (config, now) -> bufferPool.activeCount()
        );
        metrics.addMetric(
            metrics.metricName("buffer-pool-allocated-bytes", METRIC_GROUP,
                "Total bytes of direct memory allocated"),
            (Gauge<Long>) (config, now) -> bufferPool.totalAllocatedBytes()
        );
    }

    private Sensor createSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-rate", METRIC_GROUP), new Rate());
        sensor.add(metrics.metricName(name + "-total", METRIC_GROUP), new CumulativeSum());
        return sensor;
    }

    void bufferAcquired(final long bytes, final boolean poolHit) {
        if (poolHit) {
            poolHits.record(1);
        } else {
            poolMisses.record(1);
        }
        acquireBytes.record(bytes);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
