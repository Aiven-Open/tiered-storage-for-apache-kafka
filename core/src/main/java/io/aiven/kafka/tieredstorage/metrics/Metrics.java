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

import java.io.InputStream;
import java.util.List;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    private final Time time;

    private final org.apache.kafka.common.metrics.Metrics metrics;
    private final Sensor segmentCopyPerSec;
    private final Sensor segmentCopyTime;
    private final Sensor segmentFetchPerSec;
    private final Sensor segmentFetchToFirstByteFromRemoteTime;

    public Metrics(final Time time) {
        this.time = time;

        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );
        segmentCopyPerSec = metrics.sensor("segment-copy");
        final String metricGroup = "remote-storage-manager-metrics";
        segmentCopyPerSec.add(metrics.metricName("segment-copy-rate", metricGroup), new Rate());

        segmentCopyTime = metrics.sensor("segment-copy-time");
        segmentCopyTime.add(metrics.metricName("segment-copy-time-avg", metricGroup), new Avg());
        segmentCopyTime.add(metrics.metricName("segment-copy-time-max", metricGroup), new Max());

        segmentFetchPerSec = metrics.sensor("segment-fetch");
        segmentFetchPerSec.add(metrics.metricName("segment-fetch-rate", metricGroup), new Rate());

        // Measures the time between the first byte(s) of a segment was requested from the remote storage and
        // it's being available.
        segmentFetchToFirstByteFromRemoteTime = metrics.sensor("segment-fetch-to-first-byte-from-remote-time");
        segmentFetchToFirstByteFromRemoteTime.add(
            metrics.metricName("segment-fetch-to-first-byte-from-remote-time-avg", metricGroup), new Avg());
        segmentFetchToFirstByteFromRemoteTime.add(metrics.metricName(
            "segment-fetch-to-first-byte-from-remote-time-max", metricGroup), new Max());
    }

    public void recordSegmentCopy() {
        segmentCopyPerSec.record();
    }

    public void recordSegmentCopyTime(final long startMs, final long endMs) {
        segmentCopyTime.record(endMs - startMs);
    }

    public void recordSegmentFetch() {
        segmentFetchPerSec.record();
    }

    public InputStream measureInputStreamFromRemote(final InputStream inputStream) {
        return new TimeToFirstByteMeasuringInputStream(inputStream, time, segmentFetchToFirstByteFromRemoteTime);
    }

    public void close() {
        try {
            metrics.close();
        } catch (final Exception e) {
            log.warn("Error while closing metrics", e);
        }
    }
}
