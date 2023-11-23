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

package io.aiven.kafka.tieredstorage.storage.azure;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import io.aiven.kafka.tieredstorage.metrics.SensorProvider;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import reactor.core.publisher.Mono;

public class MetricCollector {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    private static final String METRIC_GROUP = "azure-blob-storage-client-metrics";

    final AzureBlobStorageConfig config;

    MetricCollector(final AzureBlobStorageConfig config) {
        this.config = config;

        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.azure")
        );
    }

    Pattern pathPattern() {
        // account is in the hostname when on azure, but included on the path when testing on Azurite
        final var maybeAccountName = "(/" + config.accountName() + ")?";
        final var exp = "^" + maybeAccountName + "/" + config.containerName() + "/" + "([^/]+)";
        return Pattern.compile(exp);
    }

    MetricsPolicy policy() {
        return new MetricsPolicy(metrics, pathPattern());
    }

    static class MetricsPolicy implements HttpPipelinePolicy {

        static final Pattern UPLOAD_QUERY_PATTERN = Pattern.compile("comp=(?<comp>[^&]+)");

        private final Sensor deleteBlobRequests;
        private final Sensor uploadBlobRequests;
        private final Sensor uploadBlockRequests;
        private final Sensor uploadBlockListRequests;
        private final Sensor getBlobRequests;

        private final Metrics metrics;
        private final Pattern pathPattern;

        MetricsPolicy(final Metrics metrics, final Pattern pathPattern) {
            this.metrics = metrics;
            this.pathPattern = pathPattern;
            this.deleteBlobRequests = createSensor("blob-delete");
            this.uploadBlobRequests = createSensor("blob-upload");
            this.uploadBlockRequests = createSensor("block-upload");
            this.uploadBlockListRequests = createSensor("block-list-upload");
            this.getBlobRequests = createSensor("blob-get");
        }

        private Sensor createSensor(final String name) {
            return new SensorProvider(metrics, name)
                .with(new MetricNameTemplate(name + "-rate", METRIC_GROUP, ""), new Rate())
                .with(new MetricNameTemplate(name + "-total", METRIC_GROUP, ""), new CumulativeCount())
                .get();
        }

        @Override
        public Mono<HttpResponse> process(final HttpPipelineCallContext context, final HttpPipelineNextPolicy next) {
            processMetrics(context);
            return next.process();
        }

        @Override
        public HttpResponse processSync(final HttpPipelineCallContext context, final HttpPipelineNextSyncPolicy next) {
            processMetrics(context);
            return next.processSync();
        }

        void processMetrics(final HttpPipelineCallContext context) {
            final var httpRequest = context.getHttpRequest();
            final var path = httpRequest.getUrl().getPath();
            if (pathPattern.matcher(path).matches()) {
                switch (httpRequest.getHttpMethod()) {
                    case GET:
                        getBlobRequests.record();
                        break;
                    case PUT:
                        final var q = httpRequest.getUrl().getQuery();
                        if (q == null) {
                            uploadBlobRequests.record();
                            break;
                        }
                        final var matcher = UPLOAD_QUERY_PATTERN.matcher(q);
                        if (matcher.find()) {
                            final var comp = matcher.group("comp");
                            switch (comp) {
                                case "block":
                                    uploadBlockRequests.record();
                                    break;
                                case "blocklist":
                                    uploadBlockListRequests.record();
                                    break;
                                default:
                            }
                        }
                        break;
                    case DELETE:
                        deleteBlobRequests.record();
                        break;
                    default:
                }
            }
        }
    }
}
