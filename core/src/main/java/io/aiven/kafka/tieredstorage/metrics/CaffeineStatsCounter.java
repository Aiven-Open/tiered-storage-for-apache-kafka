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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

public class CaffeineStatsCounter implements StatsCounter {

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
    private final String groupName;

    public CaffeineStatsCounter(final String groupName) {
        cacheHitCount = new LongAdder();
        cacheMissCount = new LongAdder();
        cacheLoadSuccessCount = new LongAdder();
        cacheLoadSuccessTimeTotal = new LongAdder();
        cacheLoadFailureCount = new LongAdder();
        cacheLoadFailureTimeTotal = new LongAdder();
        cacheEvictionCountTotal = new LongAdder();
        cacheEvictionWeightTotal = new LongAdder();

        this.groupName = groupName;

        cacheEvictionCountByCause = new ConcurrentHashMap<>();
        Arrays.stream(RemovalCause.values()).forEach(cause -> cacheEvictionCountByCause.put(cause, new LongAdder()));

        cacheEvictionWeightByCause = new ConcurrentHashMap<>();
        Arrays.stream(RemovalCause.values()).forEach(cause -> cacheEvictionWeightByCause.put(cause, new LongAdder()));

        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage.cache")
        );

        initSensor(CACHE_HITS, CACHE_HITS_TOTAL, cacheHitCount);
        initSensor(CACHE_MISSES, CACHE_MISSES_TOTAL, cacheMissCount);
        initSensor(CACHE_LOAD_SUCCESS, CACHE_LOAD_SUCCESS_TOTAL, cacheLoadSuccessCount);
        initSensor(CACHE_LOAD_SUCCESS_TIME, CACHE_LOAD_SUCCESS_TIME_TOTAL, cacheLoadSuccessTimeTotal);
        initSensor(CACHE_LOAD_FAILURE, CACHE_LOAD_FAILURE_TOTAL, cacheLoadFailureCount);
        initSensor(CACHE_LOAD_FAILURE_TIME, CACHE_LOAD_FAILURE_TIME_TOTAL, cacheLoadFailureTimeTotal);
        initSensor(CACHE_EVICTION, CACHE_EVICTION_TOTAL, cacheEvictionCountTotal);
        Arrays.stream(RemovalCause.values()).forEach(cause ->
            initSensor("cause." + cause.name() + "." + CACHE_EVICTION, CACHE_EVICTION_TOTAL,
                cacheEvictionCountByCause.get(cause), () -> Map.of("cause", cause.name()), "cause")
        );

        initSensor(CACHE_EVICTION_WEIGHT, CACHE_EVICTION_WEIGHT_TOTAL, cacheEvictionWeightTotal);

        Arrays.stream(RemovalCause.values()).forEach(cause ->
            initSensor("cause." + cause.name() + "." + CACHE_EVICTION, CACHE_EVICTION_WEIGHT_TOTAL,
                cacheEvictionWeightByCause.get(cause), () -> Map.of("cause", cause.name()), "cause")
        );
    }

    private void initSensor(final String sensorName,
                                    final String metricName,
                                    final LongAdder value,
                                    final Supplier<Map<String, String>> tagsSupplier,
                                    final String... tagNames) {
        final var name = new MetricNameTemplate(metricName, groupName, "", tagNames);
        new SensorProvider(metrics, sensorName, tagsSupplier)
            .with(name, new MeasurableValue(value))
            .get();

    }

    private void initSensor(final String sensorName, final String metricName, final LongAdder value) {
        initSensor(sensorName, metricName, value, Collections::emptyMap);
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
     * */
    private static long negativeToMaxValue(final long value) {
        return (value >= 0) ? value : Long.MAX_VALUE;
    }

    /**
     * Implementation of {@link Value} that allows fetching a value from provided instance of {@link LongAdder}
     * to avoid unnecessary calls to {@link Sensor#record()} that under the hood has a synchronized block and affects
     * performance because of that.
     */
    private static class MeasurableValue extends Value {
        private final LongAdder value;

        MeasurableValue(final LongAdder value) {
            this.value = value;
        }

        @Override
        public double measure(final MetricConfig config, final long now) {
            return value.sum();
        }
    }
}
