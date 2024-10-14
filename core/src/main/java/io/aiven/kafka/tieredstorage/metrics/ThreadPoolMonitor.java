/*
 * Copyright 2024 Aiven Oy
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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;

import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.ACTIVE_THREADS;
import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.PARALLELISM;
import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.POOL_SIZE;
import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.QUEUED_TASK_COUNT;
import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.RUNNING_THREADS;
import static io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry.STEAL_TASK_COUNT;

public class ThreadPoolMonitor {
    // only fork-join pool is supported; but could be extended to other fixed-sized pools
    final ForkJoinPool pool;
    private final Metrics metrics;
    String groupName;

    public ThreadPoolMonitor(final String groupName, final ExecutorService pool) {
        this.groupName = groupName;
        if (!(pool instanceof ForkJoinPool)) {
            throw new UnsupportedOperationException("Only ForkJoinPool supported at the moment.");
        }
        this.pool = (ForkJoinPool) pool;

        final JmxReporter reporter = new JmxReporter();
        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.thread-pool")
        );
        final var metricsRegistry = new ThreadPoolMonitorMetricsRegistry(groupName);
        registerSensor(metricsRegistry.activeThreadsTotalMetricName, ACTIVE_THREADS, this::activeThreadCount);
        registerSensor(metricsRegistry.runningThreadsTotalMetricName, RUNNING_THREADS, this::runningThreadCount);
        registerSensor(metricsRegistry.poolSizeTotalMetricName, POOL_SIZE, this::poolSize);
        registerSensor(metricsRegistry.parallelismTotalMetricName, PARALLELISM, this::parallelism);
        registerSensor(metricsRegistry.queuedTaskCountTotalMetricName, QUEUED_TASK_COUNT, this::queuedTaskCount);
        registerSensor(metricsRegistry.stealTaskCountTotalMetricName, STEAL_TASK_COUNT, this::stealTaskCount);
    }

    void registerSensor(final MetricNameTemplate metricName, final String sensorName, final Supplier<Long> supplier) {
        new SensorProvider(metrics, sensorName)
            .with(metricName, new MeasurableValue(supplier))
            .get();
    }

    long activeThreadCount() {
        return pool.getActiveThreadCount();
    }

    long runningThreadCount() {
        return pool.getRunningThreadCount();
    }

    long poolSize() {
        return pool.getPoolSize();
    }

    long parallelism() {
        return pool.getParallelism();
    }

    long queuedTaskCount() {
        return pool.getQueuedTaskCount();
    }

    long stealTaskCount() {
        return pool.getStealCount();
    }
}
