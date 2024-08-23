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

package io.aiven.kafka.tieredstorage.storage.hdfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

import com.google.common.collect.Streams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;

public class MetricCollector implements AutoCloseable {
    private static final String METRIC_GROUP = "hdfs-client-metrics";
    private static final String METRICS_CONTEXT = "aiven.kafka.server.tieredstorage.hdfs";

    private final Metrics metrics;
    private final ScheduledExecutorService executorService;
    private final Map<String, Sensor> hdfsStatToSensors;
    private final long metricsReportPeriodMs;

    MetricCollector(final long metricsReportPeriodMs) {
        final JmxReporter reporter = new JmxReporter();
        this.metrics = new Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRICS_CONTEXT)
        );

        this.metricsReportPeriodMs = metricsReportPeriodMs;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.hdfsStatToSensors = new HashMap<>();

        initSensors();
    }

    void start() {
        executorService.scheduleAtFixedRate(
            this::reportStatistics,
            metricsReportPeriodMs,
            metricsReportPeriodMs,
            TimeUnit.MILLISECONDS);
    }

    private Sensor createSensor(final String name) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricName(name + "-count", METRIC_GROUP), new Value());
        return sensor;
    }

    protected void reportStatistics() {
        Streams.stream(FileSystem.getGlobalStorageStatistics().iterator())
            .map(StorageStatistics::getLongStatistics)
            .flatMap(Streams::stream)
            .filter(statistic ->
                hdfsStatToSensors.containsKey(statistic.getName()))
            .forEach(statistic ->
                hdfsStatToSensors.get(statistic.getName()).record(statistic.getValue()));
    }

    private void initSensors() {
        hdfsStatToSensors.put(
            StorageStatistics.CommonStatisticNames.OP_CREATE,
            createSensor("file-upload"));

        hdfsStatToSensors.put(
            StorageStatistics.CommonStatisticNames.OP_DELETE,
            createSensor("file-delete"));

        hdfsStatToSensors.put(
            StorageStatistics.CommonStatisticNames.OP_OPEN,
            createSensor("file-get"));

        hdfsStatToSensors.put(
            StorageStatistics.CommonStatisticNames.OP_GET_FILE_STATUS,
            createSensor("file-get-status"));

        hdfsStatToSensors.put(
            StorageStatistics.CommonStatisticNames.OP_MKDIRS,
            createSensor("directory-create"));
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
