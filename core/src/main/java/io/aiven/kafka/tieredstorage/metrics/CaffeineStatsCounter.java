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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

public class CaffeineStatsCounter implements StatsCounter {

    static final String CACHE_HITS = "cache-hits";
    static final String CACHE_HITS_TOTAL = CACHE_HITS + "-total";
    static final String CACHE_HITS_RATE = CACHE_HITS + "-rate";
    static final String CACHE_MISSES = "cache-misses";
    static final String CACHE_MISSES_RATE = CACHE_MISSES + "-rate";
    static final String CACHE_MISSES_TOTAL = CACHE_MISSES + "-total";
    static final String CACHE_LOAD = "cache-load";
    static final String CACHE_LOAD_SUCCESS = CACHE_LOAD + "-success";
    static final String CACHE_LOAD_SUCCESS_RATE = CACHE_LOAD_SUCCESS + "-rate";
    static final String CACHE_LOAD_SUCCESS_TOTAL = CACHE_LOAD_SUCCESS + "-total";
    static final String CACHE_LOAD_SUCCESS_TIME = CACHE_LOAD_SUCCESS + "-time";
    static final String CACHE_LOAD_SUCCESS_TIME_AVG = CACHE_LOAD_SUCCESS_TIME + "-avg";
    static final String CACHE_LOAD_SUCCESS_TIME_MAX = CACHE_LOAD_SUCCESS_TIME + "-max";
    static final String CACHE_LOAD_SUCCESS_TIME_TOTAL = CACHE_LOAD_SUCCESS_TIME + "-total";
    static final String CACHE_LOAD_FAILURE = CACHE_LOAD + "-failure";
    static final String CACHE_LOAD_FAILURE_RATE = CACHE_LOAD_FAILURE + "-rate";
    static final String CACHE_LOAD_FAILURE_TOTAL = CACHE_LOAD_FAILURE + "-total";
    static final String CACHE_LOAD_FAILURE_TIME = CACHE_LOAD_FAILURE + "-time";
    static final String CACHE_LOAD_FAILURE_TIME_AVG = CACHE_LOAD_FAILURE_TIME + "-avg";
    static final String CACHE_LOAD_FAILURE_TIME_MAX = CACHE_LOAD_FAILURE_TIME + "-max";
    static final String CACHE_LOAD_FAILURE_TIME_TOTAL = CACHE_LOAD_FAILURE_TIME + "-total";

    static final String CACHE_EVICTION = "cache-eviction";
    static final String CACHE_EVICTION_TOTAL = CACHE_EVICTION + "-total";
    static final String CACHE_EVICTION_RATE = CACHE_EVICTION + "-rate";
    static final String CACHE_EVICTION_WEIGHT = CACHE_EVICTION + "-weight";
    static final String CACHE_EVICTION_WEIGHT_TOTAL = CACHE_EVICTION_WEIGHT + "-total";
    static final String CACHE_EVICTION_WEIGHT_RATE = CACHE_EVICTION_WEIGHT + "-rate";

    final MetricNameTemplate metricCacheHitsTotal;
    final MetricNameTemplate metricCacheMissesTotal;
    final MetricNameTemplate metricCacheLoadSuccessTotal;
    final MetricNameTemplate metricCacheLoadSuccessTimeTotal;
    final MetricNameTemplate metricCacheLoadFailureTotal;
    final MetricNameTemplate metricCacheLoadFailureTimeTotal;
    final MetricNameTemplate metricCacheEvictionRate;
    final MetricNameTemplate metricCacheEvictionRateByCause;
    final MetricNameTemplate metricCacheEvictionTotal;
    final MetricNameTemplate metricCacheEvictionTotalByCause;
    final MetricNameTemplate metricCacheEvictionWeight;
    final MetricNameTemplate metricCacheEvictionWeightByCause;
    final MetricNameTemplate metricCacheEvictionWeightTotal;
    final MetricNameTemplate metricCacheEvictionWeightTotalByCause;

    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final Sensor cacheHitsSensor;
    private final Sensor cacheMissesSensor;
    private final Sensor cacheLoadSuccessSensor;
    private final Sensor cacheLoadSuccessTimeSensor;
    private final Sensor cacheLoadFailureSensor;
    private final Sensor cacheLoadFailureTimeSensor;
    private final Sensor cacheEvictionSensor;
    private final Sensor cacheEvictionWeightSensor;

    public CaffeineStatsCounter(final String groupName) {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.cache")
        );

        metricCacheHitsTotal = new MetricNameTemplate(CACHE_HITS_TOTAL, groupName, "");
        cacheHitsSensor = new SensorProvider(metrics, CACHE_HITS)
            .with(new MetricNameTemplate(CACHE_HITS_RATE, groupName, ""), new Rate())
            .with(metricCacheHitsTotal, new CumulativeSum())
            .get();
        metricCacheMissesTotal = new MetricNameTemplate(CACHE_MISSES_TOTAL, groupName, "");
        cacheMissesSensor = new SensorProvider(metrics, CACHE_MISSES)
            .with(new MetricNameTemplate(CACHE_MISSES_RATE, groupName, ""), new Rate())
            .with(metricCacheMissesTotal, new CumulativeSum())
            .get();

        metricCacheLoadSuccessTotal = new MetricNameTemplate(CACHE_LOAD_SUCCESS_TOTAL, groupName, "");
        cacheLoadSuccessSensor = new SensorProvider(metrics, CACHE_LOAD_SUCCESS)
            .with(new MetricNameTemplate(CACHE_LOAD_SUCCESS_RATE, groupName, ""), new Rate())
            .with(metricCacheLoadSuccessTotal, new CumulativeSum())
            .get();
        metricCacheLoadSuccessTimeTotal =
            new MetricNameTemplate(CACHE_LOAD_SUCCESS_TIME_TOTAL, groupName, "");
        cacheLoadSuccessTimeSensor = new SensorProvider(metrics, CACHE_LOAD_SUCCESS_TIME)
            .with(new MetricNameTemplate(CACHE_LOAD_SUCCESS_TIME_AVG, groupName, ""), new Avg())
            .with(new MetricNameTemplate(CACHE_LOAD_SUCCESS_TIME_MAX, groupName, ""), new Max())
            .with(metricCacheLoadSuccessTimeTotal, new CumulativeSum())
            .get();
        metricCacheLoadFailureTotal = new MetricNameTemplate(CACHE_LOAD_FAILURE_TOTAL, groupName, "");
        cacheLoadFailureSensor = new SensorProvider(metrics, CACHE_LOAD_FAILURE)
            .with(new MetricNameTemplate(CACHE_LOAD_FAILURE_RATE, groupName, ""), new Rate())
            .with(metricCacheLoadFailureTotal, new CumulativeSum())
            .get();
        metricCacheLoadFailureTimeTotal =
            new MetricNameTemplate(CACHE_LOAD_FAILURE_TIME_TOTAL, groupName, "");
        cacheLoadFailureTimeSensor = new SensorProvider(metrics, CACHE_LOAD_FAILURE_TIME)
            .with(new MetricNameTemplate(CACHE_LOAD_FAILURE_TIME_AVG, groupName, ""), new Avg())
            .with(new MetricNameTemplate(CACHE_LOAD_FAILURE_TIME_MAX, groupName, ""), new Max())
            .with(metricCacheLoadFailureTimeTotal, new CumulativeSum())
            .get();

        metricCacheEvictionRate = new MetricNameTemplate(CACHE_EVICTION_RATE, groupName, "");
        metricCacheEvictionRateByCause = new MetricNameTemplate(CACHE_EVICTION_RATE, groupName, "", "cause");
        metricCacheEvictionTotal = new MetricNameTemplate(CACHE_EVICTION_TOTAL, groupName, "");
        metricCacheEvictionTotalByCause = new MetricNameTemplate(CACHE_EVICTION_TOTAL, groupName, "", "cause");
        cacheEvictionSensor = new SensorProvider(metrics, CACHE_EVICTION)
            .with(new MetricNameTemplate(CACHE_EVICTION_RATE, groupName, ""), new Rate())
            .with(metricCacheEvictionTotal, new CumulativeSum())
            .get();
        metricCacheEvictionWeight = new MetricNameTemplate(CACHE_EVICTION_WEIGHT_RATE, groupName, "");
        metricCacheEvictionWeightByCause = new MetricNameTemplate(CACHE_EVICTION_WEIGHT_RATE, groupName, "", "cause");
        metricCacheEvictionWeightTotal = new MetricNameTemplate(CACHE_EVICTION_WEIGHT_TOTAL, groupName, "");
        metricCacheEvictionWeightTotalByCause =
            new MetricNameTemplate(CACHE_EVICTION_WEIGHT_TOTAL, groupName, "", "cause");
        cacheEvictionWeightSensor = new SensorProvider(metrics, CACHE_EVICTION_WEIGHT)
            .with(new MetricNameTemplate(CACHE_EVICTION_WEIGHT_RATE, groupName, ""), new Rate())
            .with(metricCacheEvictionWeightTotal, new CumulativeSum())
            .get();
    }

    @Override
    public void recordHits(final int count) {
        cacheHitsSensor.record(count);
    }

    @Override
    public void recordMisses(final int count) {
        cacheMissesSensor.record(count);
    }

    @Override
    public void recordLoadSuccess(final long loadTime) {
        cacheLoadSuccessSensor.record();
        cacheLoadSuccessTimeSensor.record(loadTime);
    }

    @Override
    public void recordLoadFailure(final long loadTime) {
        cacheLoadFailureSensor.record();
        cacheLoadFailureTimeSensor.record(loadTime);
    }

    @Override
    public void recordEviction(final int weight, final RemovalCause cause) {
        cacheEvictionSensor.record();
        evictionSensorByCause(cause).record();
        cacheEvictionWeightSensor.record(weight);
        evictionWeightSensorByCause(cause).record(weight);
    }

    Sensor evictionSensorByCause(final RemovalCause cause) {
        return new SensorProvider(metrics, "cause." + cause.name() + "." + CACHE_EVICTION,
            () -> Map.of("cause", cause.name()))
            .with(metricCacheEvictionRateByCause, new Rate())
            .with(metricCacheEvictionTotalByCause, new CumulativeSum())
            .get();
    }

    Sensor evictionWeightSensorByCause(final RemovalCause cause) {
        return new SensorProvider(metrics, "cause." + cause.name() + "." + CACHE_EVICTION_WEIGHT,
            () -> Map.of("cause", cause.name()))
            .with(metricCacheEvictionWeightByCause, new Rate())
            .with(metricCacheEvictionWeightTotalByCause, new CumulativeSum())
            .get();
    }

    @Override
    public CacheStats snapshot() {
        return CacheStats.of(
            ((Double) metrics.metric(metrics.metricInstance(metricCacheHitsTotal)).metricValue()).longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheMissesTotal)).metricValue()).longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheLoadSuccessTotal)).metricValue()).longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheLoadFailureTotal)).metricValue()).longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheLoadSuccessTimeTotal)).metricValue()).longValue()
                + ((Double) metrics.metric(metrics.metricInstance(metricCacheLoadFailureTimeTotal)).metricValue())
                .longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheEvictionTotal)).metricValue()).longValue(),
            ((Double) metrics.metric(metrics.metricInstance(metricCacheEvictionWeightTotal)).metricValue()).longValue()
        );
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}
