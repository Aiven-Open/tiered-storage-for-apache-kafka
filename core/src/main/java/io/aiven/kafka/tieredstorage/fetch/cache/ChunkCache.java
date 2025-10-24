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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.config.ChunkCacheConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkKey;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitor;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

public abstract class ChunkCache<T> implements ChunkManager, Configurable {
    public static final String METRIC_GROUP = "chunk-cache-metrics";
    public static final String THREAD_POOL_METRIC_GROUP = "chunk-cache-thread-pool-metrics";

    private final ChunkManager chunkManager;
    private ExecutorService executor;

    final CaffeineStatsCounter statsCounter;

    protected AsyncCache<ChunkKey, T> cache;

    private int prefetchingSize;
    private Duration getTimeout;

    protected ChunkCache(final ChunkManager chunkManager) {
        this.chunkManager = chunkManager;
        this.statsCounter = new CaffeineStatsCounter(METRIC_GROUP);
    }

    /**
     * Fetches a specific chunk from remote storage and stores into the cache.
     * Since it's not possible to cache an opened InputStream, the actual data is cached, and everytime
     * there is a call to cache the InputStream is recreated from the data stored in cache and stored into local
     * variable. This also allows solving the race condition between eviction and fetching. Since the InputStream is
     * opened right when fetching from cache happens even if the actual value is removed from the cache,
     * the InputStream will still contain the data.
     */
    public InputStream getChunk(final ObjectKey objectKey,
                                final SegmentManifest manifest,
                                final int chunkId) throws StorageBackendException, IOException {
        final var currentChunk = manifest.chunkIndex().chunks().get(chunkId);
        startPrefetching(objectKey, manifest, currentChunk.originalPosition + currentChunk.originalSize);
        final ChunkKey chunkKey = new ChunkKey(objectKey.value(), chunkId);

        final CompletableFuture<T> future = cache.asMap().compute(chunkKey, (key, existing) -> {
            if (existing != null) {
                statsCounter.recordHit();
                return existing;
            }

            statsCounter.recordMiss();
            final CompletableFuture<T> created = CompletableFuture.supplyAsync(() -> {
                try (InputStream chunk = chunkManager.getChunk(objectKey, manifest, chunkId)) {
                    return this.cacheChunk(chunkKey, chunk);
                } catch (final StorageBackendException | IOException e) {
                    throw new CompletionException(e);
                }
            });

            created.whenComplete((r, ex) -> {
                if (ex != null) {
                    cache.asMap().remove(key, created);
                }
            });

            return created;
        });

        try {
            T t = future.get(getTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return this.cachedChunkToInputStream(t);
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

    public abstract InputStream cachedChunkToInputStream(final T cachedChunk);

    public abstract T cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException;

    public abstract RemovalListener<ChunkKey, T> removalListener();

    public abstract Weigher<ChunkKey, T> weigher();

    protected AsyncCache<ChunkKey, T> buildCache(final ChunkCacheConfig config) {
        this.executor = config.threadPoolSize().map(ForkJoinPool::new).orElse(new ForkJoinPool());
        new ThreadPoolMonitor(THREAD_POOL_METRIC_GROUP, this.executor);
        this.getTimeout = config.getTimeout();
        this.prefetchingSize = config.cachePrefetchingSize();

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

    private void startPrefetching(final ObjectKey segmentKey,
                                  final SegmentManifest segmentManifest,
                                  final int startPosition) {
        if (prefetchingSize > 0) {
            final BytesRange prefetchingRange;
            if (Integer.MAX_VALUE - startPosition < prefetchingSize) {
                prefetchingRange = BytesRange.of(startPosition, Integer.MAX_VALUE);
            } else {
                prefetchingRange = BytesRange.ofFromPositionAndSize(startPosition, prefetchingSize);
            }
            final var chunks = segmentManifest.chunkIndex().chunksForRange(prefetchingRange);
            chunks.forEach(chunk -> {
                final ChunkKey chunkKey = new ChunkKey(segmentKey.value(), chunk.id);
                cache.asMap()
                    .computeIfAbsent(chunkKey, key -> CompletableFuture.supplyAsync(() -> {
                        try {
                            final InputStream chunkStream =
                                chunkManager.getChunk(segmentKey, segmentManifest, chunk.id);
                            return this.cacheChunk(chunkKey, chunkStream);
                        } catch (final StorageBackendException | IOException e) {
                            throw new CompletionException(e);
                        }
                    }, executor));
            });
        }
    }
}
