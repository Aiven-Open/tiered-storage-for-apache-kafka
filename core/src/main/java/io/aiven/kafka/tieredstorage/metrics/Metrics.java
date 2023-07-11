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

import org.apache.kafka.common.TopicPartition;
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

import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_COPY;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_COPY_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_COPY_ERRORS;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_COPY_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_ERRORS;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_FETCH;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_FETCH_REQUESTED_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorName;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopic;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopicPartition;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicPartitionTags;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicTags;

public class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final MetricsRegistry metricsRegistry;

    public Metrics(final Time time, final MetricConfig metricConfig) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            metricConfig, List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );

        metricsRegistry = new MetricsRegistry();
    }

    public void recordSegmentCopy(final TopicPartition topicPartition, final long bytes) {
        recordSegmentCopyRequests(topicPartition);
        recordSegmentCopyBytes(topicPartition, bytes);
    }

    private void recordSegmentCopyBytes(final TopicPartition topicPartition, final long bytes) {
        new SensorProvider(metrics, sensorName(SEGMENT_COPY_BYTES))
            .with(metricsRegistry.segmentCopyBytesRate, new Rate())
            .with(metricsRegistry.segmentCopyBytesTotal, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_COPY_BYTES),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentCopyBytesRateByTopic, new Rate())
            .with(metricsRegistry.segmentCopyBytesTotalByTopic, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_COPY_BYTES),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentCopyBytesRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentCopyBytesTotalByTopicPartition, new CumulativeSum())
            .get()
            .record(bytes);
    }

    private void recordSegmentCopyRequests(final TopicPartition topicPartition) {
        new SensorProvider(metrics, sensorName(SEGMENT_COPY))
            .with(metricsRegistry.segmentCopyRequestsRate, new Rate())
            .with(metricsRegistry.segmentCopyRequestsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_COPY),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentCopyRequestsRateByTopic, new Rate())
            .with(metricsRegistry.segmentCopyRequestsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_COPY),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentCopyRequestsRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentCopyRequestsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
    }

    public void recordSegmentCopyTime(final TopicPartition topicPartition, final long startMs, final long endMs) {
        final var time = endMs - startMs;
        new SensorProvider(metrics, sensorName(SEGMENT_COPY_TIME))
            .with(metricsRegistry.segmentCopyTimeAvg, new Avg())
            .with(metricsRegistry.segmentCopyTimeMax, new Max())
            .get()
            .record(time);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_COPY_TIME),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentCopyTimeAvgByTopic, new Avg())
            .with(metricsRegistry.segmentCopyTimeMaxByTopic, new Max())
            .get()
            .record(time);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_COPY_TIME),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentCopyTimeAvgByTopicPartition, new Avg())
            .with(metricsRegistry.segmentCopyTimeMaxByTopicPartition, new Max())
            .get()
            .record(time);
    }

    public void recordSegmentCopyError(final TopicPartition topicPartition) {
        new SensorProvider(metrics, sensorName(SEGMENT_COPY_ERRORS))
            .with(metricsRegistry.segmentCopyErrorsRate, new Rate())
            .with(metricsRegistry.segmentCopyErrorsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_COPY_ERRORS),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentCopyErrorsRateByTopic, new Rate())
            .with(metricsRegistry.segmentCopyErrorsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_COPY_ERRORS),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentCopyErrorsRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentCopyErrorsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
    }

    public void recordSegmentDelete(final TopicPartition topicPartition, final long bytes) {
        recordSegmentDeleteRequests(topicPartition);
        recordSegmentDeleteBytes(topicPartition, bytes);
    }

    private void recordSegmentDeleteBytes(final TopicPartition topicPartition, final long bytes) {
        new SensorProvider(metrics, sensorName(SEGMENT_DELETE_BYTES))
            .with(metricsRegistry.segmentDeleteBytesRate, new Rate())
            .with(metricsRegistry.segmentDeleteBytesTotal, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(
            metrics,
            sensorNameByTopic(topicPartition, SEGMENT_DELETE_BYTES),
            () -> MetricsRegistry.topicTags(topicPartition))
            .with(metricsRegistry.segmentDeleteBytesRateByTopic, new Rate())
            .with(metricsRegistry.segmentDeleteBytesTotalByTopic, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(
            metrics,
            sensorNameByTopicPartition(topicPartition, SEGMENT_DELETE_BYTES),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentDeleteBytesRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentDeleteBytesTotalByTopicPartition, new CumulativeSum())
            .get()
            .record(bytes);
    }

    private void recordSegmentDeleteRequests(final TopicPartition topicPartition) {
        new SensorProvider(metrics, sensorName(SEGMENT_DELETE))
            .with(metricsRegistry.segmentDeleteRequestsRate, new Rate())
            .with(metricsRegistry.segmentDeleteRequestsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_DELETE),
            () -> MetricsRegistry.topicTags(topicPartition))
            .with(metricsRegistry.segmentDeleteRequestsRateByTopic, new Rate())
            .with(metricsRegistry.segmentDeleteRequestsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_DELETE),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentDeleteRequestsRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentDeleteRequestsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
    }

    public void recordSegmentDeleteTime(final TopicPartition topicPartition, final long startMs, final long endMs) {
        final var time = endMs - startMs;
        new SensorProvider(metrics, sensorName(SEGMENT_DELETE_TIME))
            .with(metricsRegistry.segmentDeleteTimeAvg, new Avg())
            .with(metricsRegistry.segmentDeleteTimeMax, new Max())
            .get()
            .record(time);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_DELETE_TIME),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentDeleteTimeAvgByTopic, new Avg())
            .with(metricsRegistry.segmentDeleteTimeMaxByTopic, new Max())
            .get()
            .record(time);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_DELETE_TIME),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentDeleteTimeAvgByTopicPartition, new Avg())
            .with(metricsRegistry.segmentDeleteTimeMaxByTopicPartition, new Max())
            .get()
            .record(time);
    }

    public void recordSegmentDeleteError(final TopicPartition topicPartition) {
        new SensorProvider(metrics, sensorName(SEGMENT_DELETE_ERRORS))
            .with(metricsRegistry.segmentDeleteErrorsRate, new Rate())
            .with(metricsRegistry.segmentDeleteErrorsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_DELETE_ERRORS),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentDeleteErrorsRateByTopic, new Rate())
            .with(metricsRegistry.segmentDeleteErrorsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_DELETE_ERRORS),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentDeleteErrorsRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentDeleteErrorsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
    }

    public void recordSegmentFetch(final TopicPartition topicPartition, final long bytes) {
        recordSegmentFetchRequests(topicPartition);
        recordSegmentFetchRequestedBytes(topicPartition, bytes);
    }

    private void recordSegmentFetchRequestedBytes(final TopicPartition topicPartition, final long bytes) {
        new SensorProvider(metrics, sensorName(SEGMENT_FETCH_REQUESTED_BYTES))
            .with(metricsRegistry.segmentFetchRequestedBytesRate, new Rate())
            .with(metricsRegistry.segmentFetchRequestedBytesTotal, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_FETCH_REQUESTED_BYTES),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentFetchRequestedBytesRateByTopic, new Rate())
            .with(metricsRegistry.segmentFetchRequestedBytesTotalByTopic, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_FETCH_REQUESTED_BYTES),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentFetchRequestedBytesRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentFetchRequestedBytesTotalByTopicPartition, new CumulativeSum())
            .get()
            .record(bytes);
    }

    private void recordSegmentFetchRequests(final TopicPartition topicPartition) {
        new SensorProvider(metrics, sensorName(SEGMENT_FETCH))
            .with(metricsRegistry.segmentFetchRequestsRate, new Rate())
            .with(metricsRegistry.segmentFetchRequestsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, SEGMENT_FETCH),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.segmentFetchRequestsRateByTopic, new Rate())
            .with(metricsRegistry.segmentFetchRequestsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, SEGMENT_FETCH),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.segmentFetchRequestsRateByTopicPartition, new Rate())
            .with(metricsRegistry.segmentFetchRequestsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
    }

    public void close() {
        try {
            metrics.close();
        } catch (final Exception e) {
            log.warn("Error while closing metrics", e);
        }
    }
}
