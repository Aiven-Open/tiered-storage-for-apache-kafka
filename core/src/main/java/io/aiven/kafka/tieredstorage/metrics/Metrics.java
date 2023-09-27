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

import io.aiven.kafka.tieredstorage.ObjectKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.CHUNK_DETRANSFORM_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.CHUNK_TRANSFORM_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.OBJECT_UPLOAD;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.OBJECT_UPLOAD_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_COPY_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_ERRORS;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_DELETE_TIME;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.SEGMENT_FETCH_REQUESTED_BYTES;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.objectTypeTags;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorName;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByObjectType;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopic;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopicAndObjectType;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopicPartition;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.sensorNameByTopicPartitionAndObjectType;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicAndObjectTypeTags;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicPartitionAndObjectTypeTags;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicPartitionTags;
import static io.aiven.kafka.tieredstorage.metrics.MetricsRegistry.topicTags;

public class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final MetricsRegistry metricsRegistry;

    public Metrics(final Time time, final MetricConfig metricConfig) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            metricConfig,
            List.of(reporter),
            time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );

        metricsRegistry = new MetricsRegistry();
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

    public void recordChunkTransformTime(final TopicPartition topicPartition, final long millis) {
        new SensorProvider(metrics, sensorName(CHUNK_TRANSFORM_TIME))
            .with(metricsRegistry.chunkTransformTimeAvg, new Avg())
            .with(metricsRegistry.chunkTransformTimeMax, new Max())
            .get()
            .record(millis);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, CHUNK_TRANSFORM_TIME),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.chunkTransformTimeAvgByTopic, new Avg())
            .with(metricsRegistry.chunkTransformTimeMaxByTopic, new Max())
            .get()
            .record(millis);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, CHUNK_TRANSFORM_TIME),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.chunkTransformTimeAvgByTopicPartition, new Avg())
            .with(metricsRegistry.chunkTransformTimeMaxByTopicPartition, new Max())
            .get()
            .record(millis);
    }

    public void recordChunkDetransformTime(final TopicPartition topicPartition, final long millis) {
        new SensorProvider(metrics, sensorName(CHUNK_DETRANSFORM_TIME))
            .with(metricsRegistry.chunkDetransformTimeAvg, new Avg())
            .with(metricsRegistry.chunkDetransformTimeMax, new Max())
            .get()
            .record(millis);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, CHUNK_DETRANSFORM_TIME),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.chunkDetransformTimeAvgByTopic, new Avg())
            .with(metricsRegistry.chunkDetransformTimeMaxByTopic, new Max())
            .get()
            .record(millis);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, CHUNK_DETRANSFORM_TIME),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.chunkDetransformTimeAvgByTopicPartition, new Avg())
            .with(metricsRegistry.chunkDetransformTimeMaxByTopicPartition, new Max())
            .get()
            .record(millis);
    }

    public void recordObjectUpload(final TopicPartition topicPartition, final ObjectKey.Suffix suffix,
                                   final long bytes) {
        recordObjectUploadRequests(topicPartition, suffix);
        recordObjectUploadBytes(topicPartition, suffix, bytes);
    }

    private void recordObjectUploadBytes(final TopicPartition topicPartition,
                                         final ObjectKey.Suffix suffix,
                                         final long bytes) {
        new SensorProvider(metrics, sensorName(OBJECT_UPLOAD_BYTES))
            .with(metricsRegistry.objectUploadBytesRate, new Rate())
            .with(metricsRegistry.objectUploadBytesTotal, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, OBJECT_UPLOAD_BYTES),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.objectUploadBytesRateByTopic, new Rate())
            .with(metricsRegistry.objectUploadBytesTotalByTopic, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, OBJECT_UPLOAD_BYTES),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadBytesRateByTopicPartition, new Rate())
            .with(metricsRegistry.objectUploadBytesTotalByTopicPartition, new CumulativeSum())
            .get()
            .record(bytes);
        // with suffix
        new SensorProvider(metrics, sensorNameByObjectType(suffix, OBJECT_UPLOAD_BYTES),
            () -> objectTypeTags(suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadBytesRateByObjectType, new Rate())
            .with(metricsRegistry.objectUploadBytesTotalByObjectType, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics, sensorNameByTopicAndObjectType(topicPartition, suffix, OBJECT_UPLOAD_BYTES),
            () -> topicAndObjectTypeTags(topicPartition, suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadBytesRateByTopicAndObjectType, new Rate())
            .with(metricsRegistry.objectUploadBytesTotalByTopicAndObjectType, new CumulativeSum())
            .get()
            .record(bytes);
        new SensorProvider(metrics,
            sensorNameByTopicPartitionAndObjectType(topicPartition, suffix, OBJECT_UPLOAD_BYTES),
            () -> topicPartitionAndObjectTypeTags(topicPartition, suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadBytesRateByTopicPartitionAndObjectType, new Rate())
            .with(metricsRegistry.objectUploadBytesTotalByTopicPartitionAndObjectType, new CumulativeSum())
            .get()
            .record(bytes);
    }

    private void recordObjectUploadRequests(final TopicPartition topicPartition, final ObjectKey.Suffix suffix) {
        new SensorProvider(metrics, sensorName(OBJECT_UPLOAD))
            .with(metricsRegistry.objectUploadRequestsRate, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotal, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopic(topicPartition, OBJECT_UPLOAD),
            () -> topicTags(topicPartition))
            .with(metricsRegistry.objectUploadRequestsRateByTopic, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotalByTopic, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartition(topicPartition, OBJECT_UPLOAD),
            () -> topicPartitionTags(topicPartition), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadRequestsRateByTopicPartition, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotalByTopicPartition, new CumulativeCount())
            .get()
            .record();
        // With suffix
        new SensorProvider(metrics, sensorNameByObjectType(suffix, OBJECT_UPLOAD),
            () -> objectTypeTags(suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadRequestsRateByObjectType, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotalByObjectType, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicAndObjectType(topicPartition, suffix, OBJECT_UPLOAD),
            () -> topicAndObjectTypeTags(topicPartition, suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadRequestsRateByTopicAndObjectType, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotalByTopicAndObjectType, new CumulativeCount())
            .get()
            .record();
        new SensorProvider(metrics, sensorNameByTopicPartitionAndObjectType(topicPartition, suffix, OBJECT_UPLOAD),
            () -> topicPartitionAndObjectTypeTags(topicPartition, suffix), Sensor.RecordingLevel.DEBUG)
            .with(metricsRegistry.objectUploadRequestsRateByTopicPartitionAndObjectType, new Rate())
            .with(metricsRegistry.objectUploadRequestsTotalByTopicPartitionAndObjectType, new CumulativeCount())
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
