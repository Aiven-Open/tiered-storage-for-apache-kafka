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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

public abstract class ChunkCache<T> implements ChunkManager, Configurable {
    private static final long GET_TIMEOUT_SEC = 10;
    private static final String METRIC_GROUP = "chunk-cache";

    private final ChunkManager chunkManager;
    private final ExecutorService executor = new ForkJoinPool();

    final CaffeineStatsCounter statsCounter;

    protected AsyncCache<ChunkKey, T> cache;

    protected ChunkCache(final ChunkManager chunkManager) {
        this.chunkManager = chunkManager;
        this.statsCounter = new CaffeineStatsCounter(METRIC_GROUP);
    }

    private void prefetch(final ObjectKey objectKey,
                          final SegmentManifest manifest,
                          final int chunkId) {
        final int prefetchLimit = 16 * 1024 * 1024;

        int prefetchSize = 0;

        int chunkIdFrom = -1;
        int chunkIdTo = -1;
        for (int i = chunkId + 1; i < manifest.chunkIndex().chunks().size() && prefetchSize < prefetchLimit; i++) {
            final Chunk chunk = manifest.chunkIndex().chunks().get(i);
            // Look over the next chunks to see whether they are in the cache.
            // The first not in the cache, becomes the beginning of our prefetching.
            // But if they are in the cache, we skip them, but still count their sizes.
            prefetchSize += chunk.transformedSize;
            if (chunkIdFrom == -1) {
                final ChunkKey chunkKey = new ChunkKey(objectKey.value(), i);
                if (!cache.asMap().containsKey(chunkKey)) {
                    chunkIdFrom = i;
                }
            } else {
                chunkIdTo = i;
            }
        }

        final List<CompletableFuture<T>> futures = IntStream.range(chunkIdFrom, chunkIdTo + 1)
            .mapToObj(i -> {
                final CompletableFuture<T> f = new CompletableFuture<>();
                final ChunkKey chunkKey = new ChunkKey(objectKey.value(), i);
                this.cache.put(chunkKey, f);
                return f;
            })
            .collect(Collectors.toList());

        try {
            final Iterator<InputStream> chunks = chunkManager.getChunks(objectKey, manifest, chunkIdFrom, chunkIdTo);
            int i = chunkIdFrom - 1;
            while (chunks.hasNext()) {
                final InputStream chunkContent = chunks.next();
                i += 1;
                final ChunkKey chunkKey = new ChunkKey(objectKey.value(), i);
                final T value = cacheChunk(chunkKey, chunkContent);
                futures.get(i).complete(value);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
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
        // Initiate prefetching.
        executor.submit(() -> this.prefetch(objectKey, manifest, chunkId));

        final ChunkKey chunkKey = new ChunkKey(objectKey.value(), chunkId);
        final AtomicReference<InputStream> result = new AtomicReference<>();
        try {
            return cache.asMap()
                .compute(chunkKey, (key, val) -> CompletableFuture.supplyAsync(() -> {
                    if (val == null) {
                        statsCounter.recordMiss();
                        try {
                            final InputStream chunk =
                                chunkManager.getChunk(objectKey, manifest, chunkId);
                            final T t = this.cacheChunk(chunkKey, chunk);
                            result.getAndSet(cachedChunkToInputStream(t));
                            return t;
                        } catch (final StorageBackendException | IOException e) {
                            throw new CompletionException(e);
                        }
                    } else {
                        statsCounter.recordHit();
                        try {
                            final T cachedChunk = val.get();
                            result.getAndSet(cachedChunkToInputStream(cachedChunk));
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

    public abstract InputStream cachedChunkToInputStream(final T cachedChunk);

    public abstract T cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException;

    public abstract RemovalListener<ChunkKey, T> removalListener();

    public abstract Weigher<ChunkKey, T> weigher();

    protected AsyncCache<ChunkKey, T> buildCache(final ChunkCacheConfig config) {
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
