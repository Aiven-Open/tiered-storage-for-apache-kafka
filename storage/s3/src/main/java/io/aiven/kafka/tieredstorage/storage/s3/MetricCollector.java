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

import java.util.List;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

class MetricCollector implements MetricPublisher {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    private static final String METRIC_GROUP = "s3-metrics";
    
    private final Sensor getObjectRequests;
    private final Sensor putObjectRequests;
    private final Sensor deleteObjectRequests;

    private final Sensor createMultipartUploadRequests;
    private final Sensor uploadPartRequests;
    private final Sensor completeMultipartUploadRequests;
    private final Sensor abortMultipartUploadRequests;

    MetricCollector() {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.s3")
        );
        getObjectRequests = createSensor("get-object-requests");
        putObjectRequests = createSensor("put-object-requests");
        deleteObjectRequests = createSensor("delete-object-requests");
        createMultipartUploadRequests = createSensor("create-multipart-upload-requests");
        uploadPartRequests = createSensor("upload-part-requests");
        completeMultipartUploadRequests = createSensor("complete-multipart-upload-requests");
        abortMultipartUploadRequests = createSensor("abort-multipart-upload-requests");
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
            // Try to arrange them by likelihood, at least GetObjectRequest should be first.
            if ("GetObject".equals(metricValue)) {
                this.getObjectRequests.record();
            }
            if ("UploadPart".equals(metricValue)) {
                this.uploadPartRequests.record();
            }
            if ("CreateMultipartUpload".equals(metricValue)) {
                this.createMultipartUploadRequests.record();
            }
            if ("CompleteMultipartUpload".equals(metricValue)) {
                this.completeMultipartUploadRequests.record();
            }
            if ("PutObject".equals(metricValue)) {
                this.putObjectRequests.record();
            }
            if ("DeleteObject".equals(metricValue)) {
                this.deleteObjectRequests.record();
            }
            if ("AbortMultipartUpload".equals(metricValue)) {
                this.abortMultipartUploadRequests.record();
            }
        }
    }

    @Override
    public void close() {
        metrics.close();
    }
}
