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

import java.util.List;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final Sensor segmentCopyRequests;
    private final Sensor segmentCopyBytes;
    private final Sensor segmentCopyTime;

    private final Sensor segmentDeleteRequests;
    private final Sensor segmentDeleteBytes;
    private final Sensor segmentDeleteTime;

    private final Sensor segmentFetchRequests;

    public Metrics(final Time time) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );
        final String metricGroup = "remote-storage-manager-metrics";

        segmentCopyRequests = metrics.sensor("segment-copy");
        segmentCopyRequests.add(metrics.metricName("segment-copy-rate", metricGroup), new Rate());
        segmentCopyRequests.add(metrics.metricName("segment-copy-total", metricGroup), new CumulativeCount());

        segmentCopyBytes = metrics.sensor("segment-copy-bytes");
        segmentCopyBytes.add(metrics.metricName("segment-copy-bytes-rate", metricGroup), new Rate());
        segmentCopyBytes.add(metrics.metricName("segment-copy-bytes-total", metricGroup), new CumulativeSum());

        segmentCopyTime = metrics.sensor("segment-copy-time");
        segmentCopyTime.add(metrics.metricName("segment-copy-time-avg", metricGroup), new Avg());
        segmentCopyTime.add(metrics.metricName("segment-copy-time-max", metricGroup), new Max());

        segmentDeleteRequests = metrics.sensor("segment-delete");
        segmentDeleteRequests.add(metrics.metricName("segment-delete-rate", metricGroup), new Rate());
        segmentDeleteRequests.add(metrics.metricName("segment-delete-total", metricGroup), new CumulativeCount());

        segmentDeleteBytes = metrics.sensor("segment-delete-bytes");
        segmentDeleteBytes.add(metrics.metricName("segment-delete-bytes-rate", metricGroup), new Rate());
        segmentDeleteBytes.add(metrics.metricName("segment-delete-bytes-total", metricGroup), new CumulativeSum());

        segmentDeleteTime = metrics.sensor("segment-delete-time");
        segmentDeleteTime.add(metrics.metricName("segment-delete-time-avg", metricGroup), new Avg());
        segmentDeleteTime.add(metrics.metricName("segment-delete-time-max", metricGroup), new Max());

        segmentFetchRequests = metrics.sensor("segment-fetch");
        segmentFetchRequests.add(metrics.metricName("segment-fetch-rate", metricGroup), new Rate());
        segmentFetchRequests.add(metrics.metricName("segment-fetch-total", metricGroup), new CumulativeCount());
    }

    public void recordSegmentCopy(final int bytes) {
        segmentCopyRequests.record();
        segmentCopyBytes.record(bytes);
    }

    public void recordSegmentCopyTime(final long startMs, final long endMs) {
        segmentCopyTime.record(endMs - startMs);
    }

    public void recordSegmentDelete(final int bytes) {
        segmentDeleteRequests.record();
        segmentDeleteBytes.record(bytes);
    }

    public void recordSegmentDeleteTime(final long startMs, final long endMs) {
        segmentDeleteTime.record(endMs - startMs);
    }

    public void recordSegmentFetch() {
        segmentFetchRequests.record();
    }

    public void close() {
        try {
            metrics.close();
        } catch (final Exception e) {
            log.warn("Error while closing metrics", e);
        }
    }
}
