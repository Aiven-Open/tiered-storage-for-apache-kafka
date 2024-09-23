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

package io.aiven.kafka.tieredstorage.fetch.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.kafka.tieredstorage.config.CacheConfig;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitor;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemorySegmentManifestCache implements SegmentManifestCache {
    private static final Logger log = LoggerFactory.getLogger(MemorySegmentManifestCache.class);
    private static final String METRIC_GROUP = "segment-manifest-cache-metrics";
    private static final String THREAD_POOL_METRIC_GROUP = "segment-manifest-cache-thread-pool-metrics";
    private static final long DEFAULT_MAX_SIZE = 1000L;
    private static final long DEFAULT_RETENTION_MS = 3_600_000;

    private AsyncLoadingCache<ObjectKey, SegmentManifest> cache;
    final CaffeineStatsCounter statsCounter = new CaffeineStatsCounter(METRIC_GROUP);

    final ObjectFetcher fileFetcher;
    final ObjectMapper mapper;

    Duration getTimeout;

    public MemorySegmentManifestCache(final ObjectFetcher fileFetcher, final ObjectMapper mapper) {
        this.fileFetcher = fileFetcher;
        this.mapper = mapper;
    }

    public SegmentManifest get(final ObjectKey manifestKey)
        throws StorageBackendException, IOException {
        try {
            return cache.get(manifestKey).get(getTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            // Unwrap previously wrapped exceptions if possible.
            final Throwable cause = e.getCause();

            // We don't really expect this case, but handle it nevertheless.
            if (cause == null) {
                throw new RuntimeException(e);
            }
            if (e.getCause() instanceof StorageBackendException) {
                throw (StorageBackendException) e.getCause();
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }

            throw new RuntimeException(e);
        } catch (final InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    // for testing
    RemovalListener<ObjectKey, SegmentManifest> removalListener() {
        return (key, content, cause) -> log.debug("Deleted cached value for key {} from cache."
            + " The reason of the deletion is {}", key, cause);
    }

    private static Weigher<ObjectKey, SegmentManifest> weigher() {
        return (key, value) -> 1;
    }

    protected AsyncLoadingCache<ObjectKey, SegmentManifest> buildCache(final CacheConfig config) {
        final var executor = config.threadPoolSize().map(ForkJoinPool::new).orElse(new ForkJoinPool());
        new ThreadPoolMonitor(THREAD_POOL_METRIC_GROUP, executor);
        getTimeout = config.getTimeout();

        final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        config.cacheSize().ifPresent(maximumWeight -> cacheBuilder.maximumWeight(maximumWeight).weigher(weigher()));
        config.cacheRetention().ifPresent(cacheBuilder::expireAfterAccess);
        final var cache = cacheBuilder.evictionListener(removalListener())
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .recordStats(() -> statsCounter)
            .buildAsync(key -> {
                try (final InputStream is = fileFetcher.fetch(key)) {
                    return mapper.readValue(is, SegmentManifest.class);
                }
            });

        statsCounter.registerSizeMetric(cache.synchronous()::estimatedSize);

        return cache;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final var config = CacheConfig.newBuilder(configs)
            .withDefaultSize(DEFAULT_MAX_SIZE)
            .withDefaultRetentionMs(DEFAULT_RETENTION_MS)
            .build();
        this.cache = buildCache(config);
    }
}
