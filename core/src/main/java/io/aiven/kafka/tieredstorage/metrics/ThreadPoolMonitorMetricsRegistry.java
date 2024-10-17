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

import org.apache.kafka.common.MetricNameTemplate;

public class ThreadPoolMonitorMetricsRegistry {
    public static final String METRIC_CONFIG = "aiven.kafka.server.tieredstorage.thread-pool";

    static final String ACTIVE_THREADS = "active-thread-count";
    static final String ACTIVE_THREADS_TOTAL = ACTIVE_THREADS + "-total";
    static final String ACTIVE_THREADS_TOTAL_DOC = "Number of threads currently executing tasks";
    static final String RUNNING_THREADS = "running-thread-count";
    static final String RUNNING_THREADS_TOTAL = RUNNING_THREADS + "-total";
    static final String RUNNING_THREADS_TOTAL_DOC = "Number of worker threads "
        + "that are not blocked waiting to join tasks or for other managed synchronization";
    static final String POOL_SIZE = "pool-size";
    static final String POOL_SIZE_TOTAL = POOL_SIZE + "-total";
    static final String POOL_SIZE_TOTAL_DOC = "Current number of threads in the pool";
    static final String PARALLELISM = "parallelism";
    static final String PARALLELISM_TOTAL = PARALLELISM + "-total";
    static final String PARALLELISM_TOTAL_DOC = "Targeted parallelism level of the pool";
    static final String QUEUED_TASK_COUNT = "queued-task-count";
    static final String QUEUED_TASK_COUNT_TOTAL = QUEUED_TASK_COUNT + "-total";
    static final String QUEUED_TASK_COUNT_TOTAL_DOC = "Tasks submitted to the pool "
        + "that have not yet begun executing.";
    static final String STEAL_TASK_COUNT = "steal-task-count";
    static final String STEAL_TASK_COUNT_TOTAL = STEAL_TASK_COUNT + "-total";
    static final String STEAL_TASK_COUNT_TOTAL_DOC = "Number of tasks stolen from one thread's work queue by another";

    final String groupName;
    final MetricNameTemplate activeThreadsTotalMetricName;
    final MetricNameTemplate runningThreadsTotalMetricName;
    final MetricNameTemplate poolSizeTotalMetricName;
    final MetricNameTemplate parallelismTotalMetricName;
    final MetricNameTemplate queuedTaskCountTotalMetricName;
    final MetricNameTemplate stealTaskCountTotalMetricName;

    public ThreadPoolMonitorMetricsRegistry(final String groupName) {
        this.groupName = groupName;
        activeThreadsTotalMetricName = new MetricNameTemplate(
            ACTIVE_THREADS_TOTAL,
            groupName,
            ACTIVE_THREADS_TOTAL_DOC
        );
        runningThreadsTotalMetricName = new MetricNameTemplate(
            RUNNING_THREADS_TOTAL,
            groupName,
            RUNNING_THREADS_TOTAL_DOC
        );
        poolSizeTotalMetricName = new MetricNameTemplate(
            POOL_SIZE_TOTAL,
            groupName,
            POOL_SIZE_TOTAL_DOC
        );
        parallelismTotalMetricName = new MetricNameTemplate(
            PARALLELISM_TOTAL,
            groupName,
            PARALLELISM_TOTAL_DOC
        );
        queuedTaskCountTotalMetricName = new MetricNameTemplate(
            QUEUED_TASK_COUNT_TOTAL,
            groupName,
            QUEUED_TASK_COUNT_TOTAL_DOC
        );
        stealTaskCountTotalMetricName = new MetricNameTemplate(
            STEAL_TASK_COUNT_TOTAL,
            groupName,
            STEAL_TASK_COUNT_TOTAL_DOC
        );
    }

    public List<MetricNameTemplate> all() {
        return List.of(
            activeThreadsTotalMetricName,
            runningThreadsTotalMetricName,
            poolSizeTotalMetricName,
            parallelismTotalMetricName,
            queuedTaskCountTotalMetricName,
            stealTaskCountTotalMetricName
        );
    }
}
