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

public class CaffeineMetricsRegistry {
    public static final String METRIC_CONTEXT = "aiven.kafka.server.tieredstorage.cache";

    static final String CACHE_HITS = "cache-hits";
    static final String CACHE_HITS_TOTAL = CACHE_HITS + "-total";
    static final String CACHE_MISSES = "cache-misses";
    static final String CACHE_MISSES_TOTAL = CACHE_MISSES + "-total";
    static final String CACHE_LOAD = "cache-load";
    static final String CACHE_LOAD_SUCCESS = CACHE_LOAD + "-success";
    static final String CACHE_LOAD_SUCCESS_TOTAL = CACHE_LOAD_SUCCESS + "-total";
    static final String CACHE_LOAD_SUCCESS_TIME = CACHE_LOAD_SUCCESS + "-time";
    static final String CACHE_LOAD_SUCCESS_TIME_TOTAL = CACHE_LOAD_SUCCESS_TIME + "-total";
    static final String CACHE_LOAD_FAILURE = CACHE_LOAD + "-failure";
    static final String CACHE_LOAD_FAILURE_TOTAL = CACHE_LOAD_FAILURE + "-total";
    static final String CACHE_LOAD_FAILURE_TIME = CACHE_LOAD_FAILURE + "-time";
    static final String CACHE_LOAD_FAILURE_TIME_TOTAL = CACHE_LOAD_FAILURE_TIME + "-total";

    static final String CACHE_EVICTION = "cache-eviction";
    static final String CACHE_EVICTION_TOTAL = CACHE_EVICTION + "-total";
    static final String CACHE_EVICTION_WEIGHT = CACHE_EVICTION + "-weight";
    static final String CACHE_EVICTION_WEIGHT_TOTAL = CACHE_EVICTION_WEIGHT + "-total";

    static final String CACHE_SIZE = "cache-size";
    static final String CACHE_SIZE_TOTAL = CACHE_SIZE + "-total";

    final String groupName;

    final MetricNameTemplate cacheHitsMetricName;
    final MetricNameTemplate cacheMissesMetricName;
    final MetricNameTemplate cacheLoadSuccessMetricName;
    final MetricNameTemplate cacheLoadSuccessTimeMetricName;
    final MetricNameTemplate cacheLoadFailureMetricName;
    final MetricNameTemplate cacheLoadFailureTimeMetricName;
    final MetricNameTemplate cacheEvictionMetricName;
    final MetricNameTemplate cacheEvictionByCauseMetricName;
    final MetricNameTemplate cacheEvictionWeightMetricName;
    final MetricNameTemplate cacheEvictionWeightByCauseMetricName;
    final MetricNameTemplate cacheSizeTotalMetricName;

    public CaffeineMetricsRegistry(final String groupName) {
        this.groupName = groupName;
        cacheHitsMetricName = new MetricNameTemplate(
            CACHE_HITS_TOTAL,
            groupName,
            "Cache hits"
        );
        cacheMissesMetricName = new MetricNameTemplate(
            CACHE_MISSES_TOTAL,
            groupName,
            "Cache misses"
        );
        cacheLoadSuccessMetricName = new MetricNameTemplate(
            CACHE_LOAD_SUCCESS_TOTAL,
            groupName,
            "Successful load of a new entry"
        );
        cacheLoadSuccessTimeMetricName = new MetricNameTemplate(
            CACHE_LOAD_SUCCESS_TIME_TOTAL,
            groupName,
            "Time to load a new entry"
        );
        cacheLoadFailureMetricName = new MetricNameTemplate(
            CACHE_LOAD_FAILURE_TOTAL,
            groupName,
            "Failures to load a new entry"
        );
        cacheLoadFailureTimeMetricName = new MetricNameTemplate(
            CACHE_LOAD_FAILURE_TIME_TOTAL,
            groupName,
            "Time when failing to load a new entry"
        );
        cacheEvictionMetricName = new MetricNameTemplate(
            CACHE_EVICTION_TOTAL,
            groupName,
            "Eviction of an entry from the cache"
        );
        cacheEvictionByCauseMetricName = new MetricNameTemplate(
            CACHE_EVICTION_TOTAL,
            groupName,
            "Eviction of an entry from the cache tagged by cause",
            "cause"
        );
        cacheEvictionWeightMetricName = new MetricNameTemplate(
            CACHE_EVICTION_WEIGHT_TOTAL,
            groupName,
            "Weight of evicted entry"
        );
        cacheEvictionWeightByCauseMetricName = new MetricNameTemplate(
            CACHE_EVICTION_WEIGHT_TOTAL,
            groupName,
            "Weight of evicted entry tagged by cause",
            "cause"
        );
        cacheSizeTotalMetricName = new MetricNameTemplate(
            CACHE_SIZE_TOTAL,
            groupName,
            "Estimated number of entries in the cache"
        );
    }

    public List<MetricNameTemplate> all() {
        return List.of(
            cacheHitsMetricName,
            cacheMissesMetricName,
            cacheLoadSuccessMetricName,
            cacheLoadSuccessTimeMetricName,
            cacheLoadFailureMetricName,
            cacheLoadFailureTimeMetricName,
            cacheEvictionMetricName,
            cacheEvictionByCauseMetricName,
            cacheEvictionWeightMetricName,
            cacheEvictionWeightByCauseMetricName,
            cacheSizeTotalMetricName
        );
    }
}
