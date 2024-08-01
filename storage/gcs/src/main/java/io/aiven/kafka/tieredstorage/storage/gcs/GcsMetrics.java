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

import java.util.List;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

class GcsMetrics {

    private static final String METRIC_GROUP = "gcs-metrics";

    private final Metrics metrics;
    private final Sensor fallbackReadSensor;

    GcsMetrics() {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM, new KafkaMetricsContext(
            "aiven.kafka.server.tieredstorage.gcs"
        ));
        fallbackReadSensor = metrics.sensor("fallback-read");
        fallbackReadSensor.add(metrics.metricName("fallback-read-rate", METRIC_GROUP), new Rate());
        fallbackReadSensor.add(metrics.metricName("fallback-read-count", METRIC_GROUP), new CumulativeCount());
    }

    void recordFallbackRead() {
        fallbackReadSensor.record();
    }
}
