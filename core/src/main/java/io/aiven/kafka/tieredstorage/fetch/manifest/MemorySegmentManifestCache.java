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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.kafka.tieredstorage.config.CacheConfig;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class MemorySegmentManifestCache implements SegmentManifestCache {
    public static final CacheConfig.Builder CONFIG_BUILDER = CacheConfig.newBuilder()
        .withDefaultRetention(Duration.ofHours(1).toMillis())
        .withDefaultSize(1000L);

    private static final String SEGMENT_MANIFEST_METRIC_GROUP_NAME = "segment-manifest-cache-metrics";
    private static final long GET_TIMEOUT_SEC = 10;

    private final AsyncLoadingCache<ObjectKey, SegmentManifest> cache;

    public MemorySegmentManifestCache(final CacheConfig cacheConfig,
                                      final ObjectFetcher fileFetcher,
                                      final ObjectMapper mapper,
                                      final Executor executor) {
        final var statsCounter = new CaffeineStatsCounter(SEGMENT_MANIFEST_METRIC_GROUP_NAME);
        final var cacheBuilder = Caffeine.newBuilder()
            .recordStats(() -> statsCounter)
            .executor(executor);
        cacheConfig.cacheSize().ifPresent(cacheBuilder::maximumSize);
        cacheConfig.cacheRetention().ifPresent(cacheBuilder::expireAfterWrite);
        this.cache = cacheBuilder.buildAsync(key -> {
            try (final InputStream is = fileFetcher.fetch(key)) {
                return mapper.readValue(is, SegmentManifest.class);
            }
        });
        statsCounter.registerSizeMetric(cache.synchronous()::estimatedSize);
    }

    @Override
    public SegmentManifest get(final ObjectKey manifestKey) throws StorageBackendException, IOException {
        try {
            return cache.get(manifestKey).get(GET_TIMEOUT_SEC, TimeUnit.SECONDS);
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
}
