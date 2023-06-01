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

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

class SegmentManifestProvider {
    private static final long GET_TIMEOUT_SEC = 10;

    private final ObjectKey objectKey;
    private final AsyncLoadingCache<String, SegmentManifest> cache;

    /**
     * @param maxCacheSize the max cache size (in items) or empty if the cache is unbounded.
     * @param cacheRetention the retention time of items in the cache or empty if infinite retention.
     */
    SegmentManifestProvider(final ObjectKey objectKey,
                            final Optional<Long> maxCacheSize,
                            final Optional<Duration> cacheRetention,
                            final ObjectFetcher fileFetcher,
                            final ObjectMapper mapper,
                            final Executor executor) {
        this.objectKey = objectKey;
        final var cacheBuilder = Caffeine.newBuilder()
            .executor(executor);
        maxCacheSize.ifPresent(cacheBuilder::maximumSize);
        cacheRetention.ifPresent(cacheBuilder::expireAfterWrite);
        this.cache = cacheBuilder.buildAsync(key -> {
            try (final InputStream is = fileFetcher.fetch(key)) {
                return mapper.readValue(is, SegmentManifest.class);
            }
        });
    }

    SegmentManifest get(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws StorageBackendException, IOException {
        final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);
        try {
            return cache.get(key).get(GET_TIMEOUT_SEC, TimeUnit.SECONDS);
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
