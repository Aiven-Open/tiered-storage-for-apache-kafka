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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CONFIGURED_TIMEOUT_ERRORS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECTS_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.DELETE_OBJECT_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.GET_OBJECT_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.IO_ERRORS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.IO_ERRORS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.IO_ERRORS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.OTHER_ERRORS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.OTHER_ERRORS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.OTHER_ERRORS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.PUT_OBJECT_TIME_MAX_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.SERVER_ERRORS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.SERVER_ERRORS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.SERVER_ERRORS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.THROTTLING_ERRORS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.THROTTLING_ERRORS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.THROTTLING_ERRORS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_REQUESTS;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_REQUESTS_RATE_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_TIME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_TIME_AVG_METRIC_NAME;
import static io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.UPLOAD_PART_TIME_MAX_METRIC_NAME;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.CONFIGURED_TIMEOUT;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.IO;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.OTHER;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.SERVER_ERROR;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.THROTTLING;

public class MetricCollector implements MetricPublisher {
    private static final Logger log = LoggerFactory.getLogger(MetricCollector.class);

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final Map<String, Sensor> requestMetrics = new HashMap<>();
    private final Map<String, Sensor> latencyMetrics = new HashMap<>();
    private final Map<String, Sensor> errorMetrics = new HashMap<>();

    public MetricCollector() {
        final MetricsReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.s3")
        );
        final Sensor getObjectRequestsSensor = createRequestsSensor(
            GET_OBJECT_REQUESTS,
            GET_OBJECT_REQUESTS_RATE_METRIC_NAME,
            GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("GetObject", getObjectRequestsSensor);
        final Sensor getObjectTimeSensor = createLatencySensor(
            GET_OBJECT_TIME,
            GET_OBJECT_TIME_AVG_METRIC_NAME,
            GET_OBJECT_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("GetObject", getObjectTimeSensor);
        final Sensor uploadPartRequestsSensor = createRequestsSensor(
            UPLOAD_PART_REQUESTS,
            UPLOAD_PART_REQUESTS_RATE_METRIC_NAME,
            UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("UploadPart", uploadPartRequestsSensor);
        final Sensor uploadPartTimeSensor = createLatencySensor(
            UPLOAD_PART_TIME,
            UPLOAD_PART_TIME_AVG_METRIC_NAME,
            UPLOAD_PART_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("UploadPart", uploadPartTimeSensor);
        final Sensor createMpuRequestsSensor = createRequestsSensor(
            CREATE_MULTIPART_UPLOAD_REQUESTS,
            CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("CreateMultipartUpload", createMpuRequestsSensor);
        final Sensor createMpuTimeSensor = createLatencySensor(
            CREATE_MULTIPART_UPLOAD_TIME,
            CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("CreateMultipartUpload", createMpuTimeSensor);
        final Sensor completeMpuRequestsSensor = createRequestsSensor(
            COMPLETE_MULTIPART_UPLOAD_REQUESTS,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("CompleteMultipartUpload", completeMpuRequestsSensor);
        final Sensor completeMpuTimeSensor = createLatencySensor(
            COMPLETE_MULTIPART_UPLOAD_TIME,
            COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("CompleteMultipartUpload", completeMpuTimeSensor);
        final Sensor putObjectRequestsSensor = createRequestsSensor(
            PUT_OBJECT_REQUESTS,
            PUT_OBJECT_REQUESTS_RATE_METRIC_NAME,
            PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("PutObject", putObjectRequestsSensor);
        final Sensor putObjectTimeSensor = createLatencySensor(
            PUT_OBJECT_TIME,
            PUT_OBJECT_TIME_AVG_METRIC_NAME,
            PUT_OBJECT_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("PutObject", putObjectTimeSensor);
        final Sensor deleteObjectRequestsSensor = createRequestsSensor(
            DELETE_OBJECT_REQUESTS,
            DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("DeleteObject", deleteObjectRequestsSensor);
        final Sensor deleteObjectTimeSensor = createLatencySensor(
            DELETE_OBJECT_TIME,
            DELETE_OBJECT_TIME_AVG_METRIC_NAME,
            DELETE_OBJECT_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("DeleteObject", deleteObjectTimeSensor);
        final Sensor deleteObjectsRequestsSensor = createRequestsSensor(
            DELETE_OBJECTS_REQUESTS,
            DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("DeleteObjects", deleteObjectsRequestsSensor);
        final Sensor deleteObjectsTimeSensor = createLatencySensor(
            DELETE_OBJECTS_TIME,
            DELETE_OBJECTS_TIME_AVG_METRIC_NAME,
            DELETE_OBJECTS_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("DeleteObjects", deleteObjectsTimeSensor);
        final Sensor abortMpuRequestsSensor = createRequestsSensor(
            ABORT_MULTIPART_UPLOAD_REQUESTS,
            ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME
        );
        requestMetrics.put("AbortMultipartUpload", abortMpuRequestsSensor);
        final Sensor abortMpuTimeSensor = createLatencySensor(
            ABORT_MULTIPART_UPLOAD_TIME,
            ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME
        );
        latencyMetrics.put("AbortMultipartUpload", abortMpuTimeSensor);

        final Sensor throttlingErrorsSensor = createRequestsSensor(
            THROTTLING_ERRORS,
            THROTTLING_ERRORS_RATE_METRIC_NAME,
            THROTTLING_ERRORS_TOTAL_METRIC_NAME
        );
        errorMetrics.put(THROTTLING.toString(), throttlingErrorsSensor);
        final Sensor serverErrorsSensor = createRequestsSensor(
            SERVER_ERRORS,
            SERVER_ERRORS_RATE_METRIC_NAME,
            SERVER_ERRORS_TOTAL_METRIC_NAME
        );
        errorMetrics.put(SERVER_ERROR.toString(), serverErrorsSensor);
        final Sensor configuredTimeoutErrorsSensor = createRequestsSensor(
            CONFIGURED_TIMEOUT_ERRORS,
            CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME,
            CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME
        );
        errorMetrics.put(CONFIGURED_TIMEOUT.toString(), configuredTimeoutErrorsSensor);
        final Sensor ioErrorsSensor = createRequestsSensor(
            IO_ERRORS,
            IO_ERRORS_RATE_METRIC_NAME,
            IO_ERRORS_TOTAL_METRIC_NAME
        );
        errorMetrics.put(IO.toString(), ioErrorsSensor);
        final Sensor otherErrorsSensor = createRequestsSensor(
            OTHER_ERRORS,
            OTHER_ERRORS_RATE_METRIC_NAME,
            OTHER_ERRORS_TOTAL_METRIC_NAME
        );
        errorMetrics.put(OTHER.toString(), otherErrorsSensor);
    }

    private Sensor createRequestsSensor(
        final String name,
        final MetricNameTemplate rateMetricName,
        final MetricNameTemplate totalMetricName
    ) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricInstance(rateMetricName), new Rate());
        sensor.add(metrics.metricInstance(totalMetricName), new CumulativeCount());
        return sensor;
    }

    private Sensor createLatencySensor(
        final String name,
        final MetricNameTemplate avgMetricName,
        final MetricNameTemplate maxMetricName
    ) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricInstance(maxMetricName), new Max());
        sensor.add(metrics.metricInstance(avgMetricName), new Avg());
        return sensor;
    }

    @Override
    public void publish(final MetricCollection metricCollection) {
        final List<String> metricValues = metricCollection.metricValues(CoreMetric.OPERATION_NAME);
        // metrics are reported per request, so 1 value can be assumed.
        if (metricValues.size() == 1) {
            final var metricValue = metricValues.get(0);
            final var requests = requestMetrics.get(metricValue);
            if (requests != null) {
                requests.record();
            }

            final var durations = metricCollection.metricValues(CoreMetric.API_CALL_DURATION);
            if (durations.size() == 1) {
                final var latency = latencyMetrics.get(metricValue);
                if (latency != null) {
                    latency.record(durations.get(0).toMillis());
                }
            } else {
                log.warn(
                    "Latencies included on metric collection is larger than 1: "
                        + "metric values: {} and durations: {}",
                    metricValues, durations);
            }
        } else {
            log.warn("Operations included on metric collection is larger than 1: "
                + "metric values: {}",
                metricValues);
        }

        final List<String> errorValues = metricCollection.childrenWithName("ApiCallAttempt")
            .map(metricRecords -> metricRecords.metricValues(CoreMetric.ERROR_TYPE))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        for (final String errorValue : errorValues) {
            final var sensor = errorMetrics.get(errorValue);
            if (sensor != null) {
                sensor.record();
            }
        }
    }

    @Override
    public void close() {
        metrics.close();
    }
}
