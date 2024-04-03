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

package io.aiven.kafka.tieredstorage.fetch.index;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.config.CacheConfig;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemorySegmentIndexesCache implements SegmentIndexesCache {
    private static final Logger log = LoggerFactory.getLogger(MemorySegmentIndexesCache.class);

    private static final long GET_TIMEOUT_SEC = 10;
    private static final long DEFAULT_MAX_SIZE_BYTES = 10 * 1024 * 1024;
    private static final String METRIC_GROUP = "segment-indexes-cache";

    private final Executor executor = new ForkJoinPool();
    private final CaffeineStatsCounter statsCounter = new CaffeineStatsCounter(METRIC_GROUP);

    protected AsyncCache<SegmentIndexKey, byte[]> cache;

    // for testing
    RemovalListener<SegmentIndexKey, byte[]> removalListener() {
        return (key, content, cause) -> log.debug("Deleted cached value for key {} from cache."
            + " The reason of the deletion is {}", key, cause);
    }

    private static Weigher<SegmentIndexKey, byte[]> weigher() {
        return (key, value) -> value.length;
    }

    protected AsyncCache<SegmentIndexKey, byte[]> buildCache(final CacheConfig config) {
        final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        config.cacheSize().ifPresent(maximumWeight -> cacheBuilder.maximumWeight(maximumWeight).weigher(weigher()));
        config.cacheRetention().ifPresent(cacheBuilder::expireAfterAccess);
        final var cache = cacheBuilder.evictionListener(removalListener())
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .recordStats(() -> statsCounter)
            .buildAsync();
        statsCounter.registerSizeMetric(cache.synchronous()::estimatedSize);
        return cache;
    }

    @Override
    public InputStream get(
        final ObjectKey objectKey,
        final IndexType indexType,
        final Supplier<byte[]> indexSupplier
    ) throws StorageBackendException, IOException {
        try {
            return cache.asMap()
                .compute(new SegmentIndexKey(objectKey, indexType), (key, val) -> {
                    if (val == null) {
                        statsCounter.recordMiss();
                        return CompletableFuture.supplyAsync(indexSupplier, executor);
                    } else {
                        statsCounter.recordHit();
                        return val;
                    }
                })
                .thenApplyAsync(ByteArrayInputStream::new, executor)
                .get(GET_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
            // Unwrap previously wrapped exceptions if possible.
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                cause = cause.getCause();
            }

            // We don't really expect this case, but handle it nevertheless.
            if (cause == null) {
                throw new RuntimeException(e);
            }

            if (cause instanceof StorageBackendException) {
                throw (StorageBackendException) cause;
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }

            throw new RuntimeException(e);
        } catch (final InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final var config = CacheConfig.newBuilder()
            .withDefaultSize(DEFAULT_MAX_SIZE_BYTES)
            .build(configs);
        this.cache = buildCache(config);
    }
}
