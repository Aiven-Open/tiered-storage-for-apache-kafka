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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

public abstract class AbstractChunkCache<T> extends BaseChunkCache<T> {
    private static final long GET_TIMEOUT_SEC = 10;
    private static final String METRIC_GROUP = "chunk-cache";

    private final Executor executor = new ForkJoinPool();

    private final CaffeineStatsCounter statsCounter = new CaffeineStatsCounter(METRIC_GROUP);

    protected AsyncCache<ChunkKey, T> cache;

    /**
     * Get a chunk from the cache.
     *
     * <p>If the chunk is not present in the cache, use {@literal  chunkSupplier} to get it and cache.
     *
     * <p>Since it's not possible to cache an opened InputStream, the actual data is cached, and everytime
     * there is a call to cache the InputStream is recreated from the data stored in cache and stored into local
     * variable. This also allows solving the race condition between eviction and fetching. Since the InputStream is
     * opened right when fetching from cache happens even if the actual value is removed from the cache,
     * the InputStream will still contain the data.
     */
    @Override
    protected InputStream getChunk0(final ChunkKey chunkKey,
                          final Supplier<CompletableFuture<InputStream>> chunkSupplier)
        throws ExecutionException, InterruptedException, TimeoutException {
        final AtomicReference<InputStream> result = new AtomicReference<>();
        return cache.asMap()
            .compute(chunkKey, (key, val) -> {
                if (val == null) {
                    statsCounter.recordMiss();
                    // TODO do not put a failed future into the cache
                    return chunkSupplier.get().thenApplyAsync(chunk -> {
                        try {
                            final T t = this.cacheChunk(chunkKey, chunk);
                            result.getAndSet(cachedChunkToInputStream(t));
                            return t;
                        } catch (final IOException e) {
                            throw new CompletionException(e);
                        }
                    }, executor);
                } else {
                    statsCounter.recordHit();
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            final T cachedChunk = val.get();
                            result.getAndSet(cachedChunkToInputStream(cachedChunk));
                            return cachedChunk;
                        } catch (final InterruptedException | ExecutionException e) {
                            throw new CompletionException(e);
                        }
                    }, executor);
                }
            })
            .thenApplyAsync(t -> result.get())
            .get(GET_TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    public void supplyIfAbsent(final ChunkKey chunkKey,
                               final Supplier<CompletableFuture<InputStream>> chunkSupplier) {
        // TODO do some logging if error
        // TODO do not put a failed future into the cache
        cache.asMap().computeIfAbsent(chunkKey,
            key -> chunkSupplier.get().thenApplyAsync(chunk -> {
                try {
                    return this.cacheChunk(chunkKey, chunk);
                } catch (final IOException e) {
                    throw new CompletionException(e);
                }
            }, executor));
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
