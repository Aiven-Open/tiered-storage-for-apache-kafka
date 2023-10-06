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

package io.aiven.kafka.tieredstorage.index;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.CaffeineStatsCounter;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformFinisher;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class SegmentIndexesProvider {
    private static final String SEGMENT_MANIFEST_METRIC_GROUP_NAME = "segment-indexes-cache";
    private static final long GET_TIMEOUT_SEC = 10;

    private final AsyncLoadingCache<ObjectKey, NavigableMap<Integer, Object>> cache;

    final SegmentIndexesSerde segmentIndexesSerde;
    final ObjectFetcher objectFetcher;
    final CaffeineStatsCounter statsCounter;
    final Executor executor;
    final AesEncryptionProvider encryptionProvider;

    /**
     * @param maxCacheSize   the max cache size (in items) or empty if the cache is unbounded.
     * @param cacheRetention the retention time of items in the cache or empty if infinite retention.
     */
    public SegmentIndexesProvider(final Optional<Long> maxCacheSize,
                                  final Optional<Duration> cacheRetention,
                                  final ObjectFetcher objectFetcher,
                                  final AesEncryptionProvider encryptionProvider,
                                  final Executor executor) {
        statsCounter = new CaffeineStatsCounter(SEGMENT_MANIFEST_METRIC_GROUP_NAME);
        segmentIndexesSerde = new SegmentIndexesSerde();
        this.encryptionProvider = encryptionProvider;
        this.objectFetcher = objectFetcher;
        this.executor = executor;
        final var cacheBuilder = Caffeine.newBuilder()
            .recordStats(() -> statsCounter)
            .executor(executor);
        maxCacheSize.ifPresent(cacheBuilder::maximumSize);
        cacheRetention.ifPresent(cacheBuilder::expireAfterWrite);
        this.cache = cacheBuilder.buildAsync(key -> {
            try (final InputStream is = objectFetcher.fetch(key)) {

                return segmentIndexesSerde.deserialize(is.readAllBytes());
            }
        });
        statsCounter.registerSizeMetric(cache.synchronous()::estimatedSize);
    }

    public NavigableMap<Integer, Object> get(final SegmentManifest segmentManifest,
                                             final ObjectKey indexesKey)
        throws StorageBackendException, IOException {
        try {
            return cache.asMap()
                .compute(indexesKey, (key, val) -> CompletableFuture.supplyAsync(() -> {
                    if (val == null) {
                        statsCounter.recordMiss();
                        try {
                            return fetch(segmentManifest, key);
                        } catch (final StorageBackendException e) {
                            throw new CompletionException(e);
                        }
                    } else {
                        statsCounter.recordHit();
                        try {
                            return val.get();
                        } catch (final InterruptedException | ExecutionException e) {
                            throw new CompletionException(e);
                        }
                    }
                }, executor))
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

    NavigableMap<Integer, Object> fetch(final SegmentManifest segmentManifest,
                                        final ObjectKey objectKey) throws StorageBackendException {
        final var content = objectFetcher.fetch(objectKey);
        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(content);
        final Optional<SegmentEncryptionMetadata> encryptionMetadata = segmentManifest.encryption();
        if (encryptionMetadata.isPresent()) {
            detransformEnum = new DecryptionChunkEnumeration(
                detransformEnum,
                encryptionMetadata.get().ivSize(),
                encryptedChunk -> encryptionProvider.decryptionCipher(encryptedChunk, encryptionMetadata.get())
            );
        }
        final DetransformFinisher detransformFinisher = new DetransformFinisher(detransformEnum);
        try (final var inputStream = detransformFinisher.toInputStream()) {
            return segmentIndexesSerde.deserialize(inputStream.readAllBytes());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
