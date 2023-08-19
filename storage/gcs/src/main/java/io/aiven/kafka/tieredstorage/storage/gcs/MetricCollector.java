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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpMethods;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;

class MetricCollector {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    /**
     * The pattern for object metadata paths.
     *
     * <p>That is, {@literal "/storage/v1/b/<bucket>/o/<object>"}.
     */
    private static final Pattern OBJECT_METADATA_PATH_PATTERN =
        Pattern.compile("^/storage/v1/b/\\w+/o/\\w+/?$");

    /**
     * The pattern for object download paths.
     *
     * <p>That is, {@literal "/download/storage/v1/b/<bucket>/o/<object>"}.
     */
    private static final Pattern OBJECT_DOWNLOAD_PATH_PATTERN =
        Pattern.compile("^/download/storage/v1/b/\\w+/o/\\w+/?$");

    /**
     * The pattern for object upload paths.
     *
     * <p>That is, {@literal "/upload/storage/v1/b/<bucket>/o"}.
     */
    private static final Pattern OBJECT_UPLOAD_PATH_PATTERN =
        Pattern.compile("^/upload/storage/v1/b/\\w+/o/?$");

    private static final String METRIC_GROUP = "gcs-metrics";

    private final Sensor getObjectMetadataRequests;
    private final Sensor deleteObjectRequests;
    private final Sensor resumableUploadInitiateRequests;
    private final Sensor resumableChunkUploadRequests;
    private final Sensor getObjectRequests;

    MetricCollector() {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.gcs")
        );

        getObjectMetadataRequests = createSensor("object-metadata-get");
        getObjectRequests = createSensor("object-get");
        deleteObjectRequests = createSensor("object-delete");
        resumableUploadInitiateRequests = createSensor("resumable-upload-initiate");
        resumableChunkUploadRequests = createSensor("resumable-chunk-upload");
    }

    private Sensor createSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-rate", METRIC_GROUP), new Rate());
        sensor.add(metrics.metricName(name + "-total", METRIC_GROUP), new CumulativeCount());
        return sensor;
    }

    private final MetricResponseInterceptor metricResponseInterceptor = new MetricResponseInterceptor();

    private class MetricResponseInterceptor implements HttpResponseInterceptor {

        @Override
        public void interceptResponse(final HttpResponse response) throws IOException {
            final HttpRequest request = response.getRequest();
            final GenericUrl url = request.getUrl();

            if (OBJECT_METADATA_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Single object metadata operations: metadata gets and object deletions.
                if (request.getRequestMethod().equals(HttpMethods.GET)) {
                    getObjectMetadataRequests.record();
                } else if (request.getRequestMethod().equals(HttpMethods.DELETE)) {
                    deleteObjectRequests.record();
                }
            } else if (OBJECT_DOWNLOAD_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Object download operations.
                if (request.getRequestMethod().equals(HttpMethods.GET)) {
                    getObjectRequests.record();
                }
            } else if (OBJECT_UPLOAD_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Object upload operations.
                if (request.getRequestMethod().equals(HttpMethods.POST)
                    && url.getFirst("uploadType").equals("resumable")) {
                    resumableUploadInitiateRequests.record();
                } else if (request.getRequestMethod().equals(HttpMethods.PUT)
                    && url.getFirst("uploadType").equals("resumable")
                    && url.containsKey("upload_id")) {
                    resumableChunkUploadRequests.record();
                }
            }
        }
    }

    HttpTransportOptions httpTransportOptions() {
        return new HttpTransportOptions(HttpTransportOptions.newBuilder()) {
            @Override
            public HttpRequestInitializer getHttpRequestInitializer(final ServiceOptions<?, ?> serviceOptions) {
                final var superInitializer = super.getHttpRequestInitializer(serviceOptions);
                return request -> {
                    superInitializer.initialize(request);
                    request.setResponseInterceptor(metricResponseInterceptor);
                };
            }
        };
    }
}
