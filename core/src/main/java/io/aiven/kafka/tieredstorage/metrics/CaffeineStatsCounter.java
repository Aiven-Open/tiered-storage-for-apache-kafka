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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_EVICTION;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_EVICTION_WEIGHT;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_HITS;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_LOAD_FAILURE;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_LOAD_FAILURE_TIME;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_LOAD_SUCCESS;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_LOAD_SUCCESS_TIME;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_MISSES;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.CACHE_SIZE;
import static io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry.METRIC_CONTEXT;

/**
 * Records cache metrics managed by Caffeine {@code Cache#stats}.
 *
 * <p>Note that this doesn't instrument the cache's size by default. Use
 * {@link #registerSizeMetric(Supplier)} to do so after the cache has been built.
 * Similar approach used by
 * <a href="https://github.com/micrometer-metrics/micrometer/blob/main/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/cache/CaffeineStatsCounter.java">Micrometer</a>
 */
public class CaffeineStatsCounter implements StatsCounter {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    private final LongAdder cacheHitCount;
    private final LongAdder cacheMissCount;
    private final LongAdder cacheLoadSuccessCount;
    private final LongAdder cacheLoadSuccessTimeTotal;
    private final LongAdder cacheLoadFailureCount;
    private final LongAdder cacheLoadFailureTimeTotal;
    private final LongAdder cacheEvictionCountTotal;
    private final LongAdder cacheEvictionWeightTotal;
    private final ConcurrentHashMap<RemovalCause, LongAdder> cacheEvictionCountByCause;
    private final ConcurrentHashMap<RemovalCause, LongAdder> cacheEvictionWeightByCause;
    private final CaffeineMetricsRegistry metricsRegistry;

    public CaffeineStatsCounter(final String groupName) {
        cacheHitCount = new LongAdder();
        cacheMissCount = new LongAdder();
        cacheLoadSuccessCount = new LongAdder();
        cacheLoadSuccessTimeTotal = new LongAdder();
        cacheLoadFailureCount = new LongAdder();
        cacheLoadFailureTimeTotal = new LongAdder();
        cacheEvictionCountTotal = new LongAdder();
        cacheEvictionWeightTotal = new LongAdder();

        cacheEvictionCountByCause = new ConcurrentHashMap<>();
        Arrays.stream(RemovalCause.values()).forEach(cause -> cacheEvictionCountByCause.put(cause, new LongAdder()));

        cacheEvictionWeightByCause = new ConcurrentHashMap<>();
        Arrays.stream(RemovalCause.values()).forEach(cause -> cacheEvictionWeightByCause.put(cause, new LongAdder()));

        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        metricsRegistry = new CaffeineMetricsRegistry(groupName);
        initSensor(
            metricsRegistry.cacheHitsMetricName,
            CACHE_HITS,
            cacheHitCount
        );
        initSensor(
            metricsRegistry.cacheMissesMetricName,
            CACHE_MISSES,
            cacheMissCount
        );
        initSensor(
            metricsRegistry.cacheLoadSuccessMetricName,
            CACHE_LOAD_SUCCESS,
            cacheLoadSuccessCount
        );
        initSensor(
            metricsRegistry.cacheLoadSuccessTimeMetricName,
            CACHE_LOAD_SUCCESS_TIME,
            cacheLoadSuccessTimeTotal
        );
        initSensor(
            metricsRegistry.cacheLoadFailureMetricName,
            CACHE_LOAD_FAILURE,
            cacheLoadFailureCount
        );
        initSensor(
            metricsRegistry.cacheLoadFailureTimeMetricName,
            CACHE_LOAD_FAILURE_TIME,
            cacheLoadFailureTimeTotal
        );
        initSensor(
            metricsRegistry.cacheEvictionMetricName,
            CACHE_EVICTION,
            cacheEvictionCountTotal
        );
        Arrays.stream(RemovalCause.values()).forEach(cause ->
            initSensor(
                metricsRegistry.cacheEvictionByCauseMetricName,
                "cause." + cause.name() + "." + CACHE_EVICTION,
                cacheEvictionCountByCause.get(cause),
                () -> Map.of("cause", cause.name())
            )
        );

        initSensor(
            metricsRegistry.cacheEvictionWeightMetricName,
            CACHE_EVICTION_WEIGHT,
            cacheEvictionWeightTotal
        );

        Arrays.stream(RemovalCause.values()).forEach(cause ->
            initSensor(
                metricsRegistry.cacheEvictionWeightByCauseMetricName,
                "cause." + cause.name() + "." + CACHE_EVICTION,
                cacheEvictionWeightByCause.get(cause),
                () -> Map.of("cause", cause.name())
            )
        );
    }

    private void initSensor(
        final MetricNameTemplate metricNameTemplate,
        final String sensorName,
        final LongAdder value,
        final Supplier<Map<String, String>> tagsSupplier
    ) {
        new SensorProvider(metrics, sensorName, tagsSupplier)
            .with(metricNameTemplate, new MeasurableValue(value::sum))
            .get();
    }

    private void initSensor(final MetricNameTemplate metricNameTemplate, final String sensorName,
                            final LongAdder value) {
        initSensor(metricNameTemplate, sensorName, value, Collections::emptyMap);
    }

    @Override
    public void recordHits(final int count) {
        cacheHitCount.add(count);
    }

    @Override
    public void recordMisses(final int count) {
        cacheMissCount.add(count);
    }

    @Override
    public void recordLoadSuccess(final long loadTime) {
        cacheLoadSuccessCount.increment();
        cacheLoadSuccessTimeTotal.add(loadTime);
    }

    @Override
    public void recordLoadFailure(final long loadTime) {
        cacheLoadFailureCount.increment();
        cacheLoadFailureTimeTotal.add(loadTime);
    }

    @Override
    public void recordEviction(final int weight, final RemovalCause cause) {
        cacheEvictionCountTotal.increment();
        cacheEvictionCountByCause.get(cause).increment();
        cacheEvictionWeightTotal.add(weight);
        cacheEvictionWeightByCause.get(cause).add(weight);
    }

    /**
     * Not called by Caffeine directory. Used to record custom cache miss.
     */
    public void recordMiss() {
        cacheMissCount.increment();
    }

    /**
     * Not called by Caffeine directory. Used to record custom cache hit.
     */
    public void recordHit() {
        cacheHitCount.increment();
    }

    /**
     * Includes a cache size to the reported metrics.
     *
     * <p>Needs additional registration as operation to provide this value is not part of
     * the {@code CaffeineStatsCounter} interface.
     *
     * @param sizeSupplier operation from cache to provide cache size value
     */
    public void registerSizeMetric(final Supplier<Long> sizeSupplier) {
        new SensorProvider(metrics, CACHE_SIZE)
            .with(metricsRegistry.cacheSizeTotalMetricName, new MeasurableValue(sizeSupplier))
            .get();
    }

    @Override
    public CacheStats snapshot() {
        return CacheStats.of(
            negativeToMaxValue(cacheHitCount.sum()),
            negativeToMaxValue(cacheMissCount.sum()),
            negativeToMaxValue(cacheLoadSuccessCount.sum()),
            negativeToMaxValue(cacheLoadFailureCount.sum()),
            negativeToMaxValue(cacheLoadSuccessTimeTotal.sum() + cacheLoadFailureTimeTotal.sum()),
            negativeToMaxValue(cacheEvictionCountTotal.sum()),
            negativeToMaxValue(cacheEvictionWeightTotal.sum())
        );
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }

    /**
     * Prevents passing negative values to {@link  CacheStats} in case of long overflow.
     * Returns {@code value}, if non-negative. Otherwise, returns {@link Long#MAX_VALUE}.
     */
    private static long negativeToMaxValue(final long value) {
        return (value >= 0) ? value : Long.MAX_VALUE;
    }

}
