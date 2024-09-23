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

public class ThreadPoolMonitor {
    // only fork-join pool is supported; but could be extended to other fixed-sized pools
    final ForkJoinPool pool;
    private final Metrics metrics;
    String groupName;

    private static final String ACTIVE_THREADS = "active-thread-count";
    private static final String ACTIVE_THREADS_TOTAL = ACTIVE_THREADS + "-total";
    private static final String RUNNING_THREADS = "running-thread-count";
    private static final String RUNNING_THREADS_TOTAL = RUNNING_THREADS + "-total";
    private static final String POOL_SIZE = "pool-size";
    private static final String POOL_SIZE_TOTAL = POOL_SIZE + "-total";
    private static final String PARALLELISM = "parallelism";
    private static final String PARALLELISM_TOTAL = PARALLELISM + "-total";
    private static final String QUEUED_TASK_COUNT = "queued-task-count";
    private static final String QUEUED_TASK_COUNT_TOTAL = QUEUED_TASK_COUNT + "-total";
    private static final String STEAL_TASK_COUNT = "steal-task-count";
    private static final String STEAL_TASK_COUNT_TOTAL = STEAL_TASK_COUNT + "-total";

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

        registerSensor(ACTIVE_THREADS_TOTAL, ACTIVE_THREADS, this::activeThreadCount);
        registerSensor(RUNNING_THREADS_TOTAL, RUNNING_THREADS, this::runningThreadCount);
        registerSensor(POOL_SIZE_TOTAL, POOL_SIZE, this::poolSize);
        registerSensor(PARALLELISM_TOTAL, PARALLELISM, this::parallelism);
        registerSensor(QUEUED_TASK_COUNT_TOTAL, QUEUED_TASK_COUNT, this::queuedTaskCount);
        registerSensor(STEAL_TASK_COUNT_TOTAL, STEAL_TASK_COUNT, this::stealTaskCount);
    }

    void registerSensor(final String metricName, final String sensorName, final Supplier<Long> supplier) {
        final var name = new MetricNameTemplate(metricName, groupName, "");
        new SensorProvider(metrics, sensorName)
            .with(name, new MeasurableValue(supplier))
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
