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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.CONFIGURED_TIMEOUT;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.IO;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.OTHER;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.SERVER_ERROR;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.THROTTLING;

class MetricCollector implements MetricPublisher {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    private static final String METRIC_GROUP = "s3-metrics";
    private final Map<String, Sensor> requestMetrics = new HashMap<>();
    private final Map<String, Sensor> errorMetrics = new HashMap<>();

    MetricCollector() {
        final MetricsReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.s3")
        );
        requestMetrics.put("GetObject", createSensor("get-object-requests"));
        requestMetrics.put("UploadPart", createSensor("upload-part-requests"));
        requestMetrics.put("CreateMultipartUpload", createSensor("create-multipart-upload-requests"));
        requestMetrics.put("CompleteMultipartUpload", createSensor("complete-multipart-upload-requests"));
        requestMetrics.put("PutObject", createSensor("put-object-requests"));
        requestMetrics.put("DeleteObject", createSensor("delete-object-requests"));
        requestMetrics.put("AbortMultipartUpload", createSensor("abort-multipart-upload-requests"));

        errorMetrics.put(THROTTLING.name(), createSensor("throttling-errors"));
        errorMetrics.put(SERVER_ERROR.name(), createSensor("server-errors"));
        errorMetrics.put(CONFIGURED_TIMEOUT.name(), createSensor("configure-timeout-errors"));
        errorMetrics.put(IO.name(), createSensor("io-errors"));
        errorMetrics.put(OTHER.name(), createSensor("other-errors"));
    }

    private Sensor createSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-rate", METRIC_GROUP), new Rate());
        sensor.add(metrics.metricName(name + "-total", METRIC_GROUP), new CumulativeCount());
        return sensor;
    }

    @Override
    public void publish(final MetricCollection metricCollection) {
        final List<String> metricValues = metricCollection.metricValues(CoreMetric.OPERATION_NAME);
        for (final String metricValue : metricValues) {
            if (requestMetrics.containsKey(metricValue)) {
                requestMetrics.get(metricValue).record();
            }
        }
        final List<String> errorValues = metricCollection.metricValues(CoreMetric.ERROR_TYPE);
        for (final String errorValue : errorValues) {
            if (errorMetrics.containsKey(errorValue)) {
                errorMetrics.get(errorValue).record();
            }
        }
    }

    @Override
    public void close() {
        metrics.close();
    }
}
