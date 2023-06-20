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

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.AWSRequestMetrics;

class MetricCollector extends RequestMetricCollector {
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
    public void collectMetrics(final Request<?> request, final Response<?> response) {
        final AWSRequestMetrics awsRequestMetrics = request.getAWSRequestMetrics();
        if (awsRequestMetrics == null) {
            return;
        }
        final List<Object> requestTypes = awsRequestMetrics.getProperty(AWSRequestMetrics.Field.RequestType);
        if (requestTypes == null || requestTypes.isEmpty()) {
            return;
        }
        final Object requestType = requestTypes.get(0);
        if (requestType == null) {
            return;
        }

        // Try to arrange them by likelihood, at least GetObjectRequest should be first.
        if (GetObjectRequest.class.getSimpleName().equals(requestType)) {
            this.getObjectRequests.record();
        } else if (UploadPartRequest.class.getSimpleName().equals(requestType)) {
            this.uploadPartRequests.record();
        } else if (InitiateMultipartUploadRequest.class.getSimpleName().equals(requestType)) {
            this.createMultipartUploadRequests.record();
        } else if (CompleteMultipartUploadRequest.class.getSimpleName().equals(requestType)) {
            this.completeMultipartUploadRequests.record();
        } else if (PutObjectRequest.class.getSimpleName().equals(requestType)) {
            this.putObjectRequests.record();
        } else if (DeleteObjectRequest.class.getSimpleName().equals(requestType)) {
            this.deleteObjectRequests.record();
        } else if (AbortMultipartUploadRequest.class.getSimpleName().equals(requestType)) {
            this.abortMultipartUploadRequests.record();
        }
    }
}
