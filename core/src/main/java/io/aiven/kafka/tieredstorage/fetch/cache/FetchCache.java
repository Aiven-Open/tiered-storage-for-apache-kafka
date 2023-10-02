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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.fetch.FetchManager;
import io.aiven.kafka.tieredstorage.fetch.FetchPart;
import io.aiven.kafka.tieredstorage.fetch.FetchPartKey;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

public abstract class FetchCache<T> implements FetchManager, Configurable {
    private static final long GET_TIMEOUT_SEC = 10;
    private static final String METRIC_GROUP = "fetch-cache";

    private final FetchManager fetchManager;
    private final Executor executor = new ForkJoinPool();

    final CaffeineStatsCounter statsCounter;

    protected AsyncCache<FetchPartKey, T> cache;

    protected FetchCache(final FetchManager fetchManager) {
        this.fetchManager = fetchManager;
        this.statsCounter = new CaffeineStatsCounter(METRIC_GROUP);
    }

    /**
     * Fetches chunks of a specific range defined by {@code Part} from remote storage and stores into the cache.
     * Since it's not possible to cache an opened InputStream, the actual data is cached, and everytime
     * there is a call to cache the InputStream is recreated from the data stored in cache and stored into local
     * variable. This also allows solving the race condition between eviction and fetching. Since the InputStream is
     * opened right when fetching from cache happens even if the actual value is removed from the cache,
     * the InputStream will still contain the data.
     */
    @Override
    public InputStream fetchPartContent(final ObjectKey objectKey,
                                        final SegmentManifest manifest,
                                        final FetchPart part) throws StorageBackendException, IOException {
        final FetchPartKey fetchPartKey = new FetchPartKey(objectKey.value(), part.range);
        final AtomicReference<InputStream> result = new AtomicReference<>();
        try {
            return cache.asMap()
                .compute(fetchPartKey, (key, val) -> CompletableFuture.supplyAsync(() -> {
                    if (val == null) {
                        statsCounter.recordMiss();
                        try {
                            final InputStream partContent = fetchManager.fetchPartContent(objectKey, manifest, part);
                            final T t = this.cachePartContent(fetchPartKey, partContent);
                            result.getAndSet(readCachedPartContent(t));
                            return t;
                        } catch (final StorageBackendException | IOException e) {
                            throw new CompletionException(e);
                        }
                    } else {
                        statsCounter.recordHit();
                        try {
                            final T cachedChunk = val.get();
                            result.getAndSet(readCachedPartContent(cachedChunk));
                            return cachedChunk;
                        } catch (final InterruptedException | ExecutionException e) {
                            throw new CompletionException(e);
                        }
                    }
                }, executor))
                .thenApplyAsync(t -> result.get())
                .get(GET_TIMEOUT_SEC, TimeUnit.SECONDS);
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


    public void prepareParts(final ObjectKey objectKey,
                             final SegmentManifest manifest,
                             final Set<FetchPart> parts) {
        for (final var part : parts) {
            final FetchPartKey fetchPartKey = new FetchPartKey(objectKey.value(), part.range);
            cache.asMap()
                .computeIfAbsent(fetchPartKey, key -> CompletableFuture.supplyAsync(() -> {
                    statsCounter.recordMiss();
                    try {
                        final InputStream partContent = fetchManager.fetchPartContent(objectKey, manifest, part);
                        return this.cachePartContent(fetchPartKey, partContent);
                    } catch (final StorageBackendException | IOException e) {
                        throw new CompletionException(e);
                    }
                }, executor));
        }
    }

    public abstract InputStream readCachedPartContent(final T cachedChunk);

    public abstract T cachePartContent(final FetchPartKey fetchPartKey, final InputStream chunk) throws IOException;

    public abstract RemovalListener<FetchPartKey, T> removalListener();

    public abstract Weigher<FetchPartKey, T> weigher();

    protected AsyncCache<FetchPartKey, T> buildCache(final FetchCacheConfig config) {
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
}
