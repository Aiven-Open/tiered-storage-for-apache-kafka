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

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * Inspired by <a href="https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/SensorBuilder.java">SensorBuilder</a>
 */
public class SensorProvider {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final Sensor sensor;

    private final boolean preexisting;

    private final Map<String, String> tags;

    public SensorProvider(final Metrics metrics,
                          final String name) {
        this(metrics, name, Collections::emptyMap, Sensor.RecordingLevel.INFO);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Sensor.RecordingLevel recordingLevel) {
        this(metrics, name, Collections::emptyMap, recordingLevel);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Supplier<Map<String, String>> tagsSupplier) {
        this(metrics, name, tagsSupplier, Sensor.RecordingLevel.INFO);
    }

    public SensorProvider(final Metrics metrics,
                          final String name,
                          final Supplier<Map<String, String>> tagsSupplier,
                          final Sensor.RecordingLevel recordingLevel) {
        this.metrics = metrics;
        final Sensor s = metrics.getSensor(name);

        if (s != null) {
            sensor = s;
            tags = Collections.emptyMap();
            preexisting = true;
        } else {
            sensor = metrics.sensor(name, recordingLevel);
            tags = tagsSupplier.get();
            preexisting = false;
        }
    }

    public SensorProvider with(final MetricNameTemplate name, final MeasurableStat stat) {
        if (!preexisting) {
            sensor.add(metrics.metricInstance(name, tags), stat);
        }

        return this;
    }

    public Sensor get() {
        return sensor;
    }
}
