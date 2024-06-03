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

import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.CONFIGURED_TIMEOUT;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.IO;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.OTHER;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.SERVER_ERROR;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.THROTTLING;

class MetricCollector implements MetricPublisher {
    private static final Logger log = LoggerFactory.getLogger(MetricCollector.class);

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private static final String METRIC_GROUP = "s3-client-metrics";
    private final Map<String, Sensor> requestMetrics = new HashMap<>();
    private final Map<String, Sensor> latencyMetrics = new HashMap<>();
    private final Map<String, Sensor> errorMetrics = new HashMap<>();

    MetricCollector() {
        final MetricsReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.s3")
        );
        requestMetrics.put("GetObject", createRequestsSensor("get-object-requests"));
        latencyMetrics.put("GetObject", createLatencySensor("get-object-time"));
        requestMetrics.put("UploadPart", createRequestsSensor("upload-part-requests"));
        latencyMetrics.put("UploadPart", createLatencySensor("upload-part-time"));
        requestMetrics.put("CreateMultipartUpload", createRequestsSensor("create-multipart-upload-requests"));
        latencyMetrics.put("CreateMultipartUpload", createLatencySensor("create-multipart-upload-time"));
        requestMetrics.put("CompleteMultipartUpload", createRequestsSensor("complete-multipart-upload-requests"));
        latencyMetrics.put("CompleteMultipartUpload", createLatencySensor("complete-multipart-upload-time"));
        requestMetrics.put("PutObject", createRequestsSensor("put-object-requests"));
        latencyMetrics.put("PutObject", createLatencySensor("put-object-time"));
        requestMetrics.put("DeleteObject", createRequestsSensor("delete-object-requests"));
        latencyMetrics.put("DeleteObject", createLatencySensor("delete-object-time"));
        requestMetrics.put("DeleteObjects", createRequestsSensor("delete-objects-requests"));
        latencyMetrics.put("DeleteObjects", createLatencySensor("delete-objects-time"));
        requestMetrics.put("AbortMultipartUpload", createRequestsSensor("abort-multipart-upload-requests"));
        latencyMetrics.put("AbortMultipartUpload", createLatencySensor("abort-multipart-upload-time"));

        errorMetrics.put(THROTTLING.toString(), createRequestsSensor("throttling-errors"));
        errorMetrics.put(SERVER_ERROR.toString(), createRequestsSensor("server-errors"));
        errorMetrics.put(CONFIGURED_TIMEOUT.toString(), createRequestsSensor("configured-timeout-errors"));
        errorMetrics.put(IO.toString(), createRequestsSensor("io-errors"));
        errorMetrics.put(OTHER.toString(), createRequestsSensor("other-errors"));
    }

    private Sensor createRequestsSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-rate", METRIC_GROUP), new Rate());
        sensor.add(metrics.metricName(name + "-total", METRIC_GROUP), new CumulativeCount());
        return sensor;
    }

    private Sensor createLatencySensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-max", METRIC_GROUP), new Max());
        sensor.add(metrics.metricName(name + "-avg", METRIC_GROUP), new Avg());
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
