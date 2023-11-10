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

package io.aiven.kafka.tieredstorage.chunkmanager;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformFinisher;

public class DefaultChunkManager implements ChunkManager {
    private final ObjectFetcher fetcher;
    private final AesEncryptionProvider aesEncryptionProvider;
    final ChunkCache<?> chunkCache;
    private final int prefetchingSize;

    private final Executor executor = new ForkJoinPool();

    public DefaultChunkManager(final ObjectFetcher fetcher,
                               final AesEncryptionProvider aesEncryptionProvider,
                               final ChunkCache<?> chunkCache,
                               final int prefetchingSize) {
        this.fetcher = fetcher;
        this.aesEncryptionProvider = aesEncryptionProvider;
        this.chunkCache = chunkCache;
        this.prefetchingSize = prefetchingSize;
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e., decrypted and decompressed).
     */
    public InputStream getChunk(final ObjectKey objectKey, final SegmentManifest manifest,
                                final int chunkId) throws StorageBackendException, IOException {
        final var currentChunk = manifest.chunkIndex().chunks().get(chunkId);
        startPrefetching(objectKey, manifest, currentChunk.originalPosition + currentChunk.originalSize);

        final ChunkKey chunkKey = new ChunkKey(objectKey.value(), chunkId);
        return chunkCache.getChunk(chunkKey, createChunkSupplier(objectKey, manifest, chunkId));
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
                chunkCache.supplyIfAbsent(chunkKey, createChunkSupplier(segmentKey, segmentManifest, chunk.id));
            });
        }
    }

    private Supplier<CompletableFuture<InputStream>> createChunkSupplier(final ObjectKey objectKey,
                                                                         final SegmentManifest manifest,
                                                                         final int chunkId) {
        return () -> CompletableFuture.supplyAsync(() -> {
            final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);

            final InputStream chunkContent;
            try {
                chunkContent = fetcher.fetch(objectKey, chunk.range());
            } catch (final StorageBackendException e) {
                throw new CompletionException(e);
            }

            DetransformChunkEnumeration detransformEnum =
                new BaseDetransformChunkEnumeration(chunkContent, List.of(chunk));
            final Optional<SegmentEncryptionMetadata> encryptionMetadata = manifest.encryption();
            if (encryptionMetadata.isPresent()) {
                detransformEnum = new DecryptionChunkEnumeration(
                    detransformEnum,
                    encryptionMetadata.get().ivSize(),
                    encryptedChunk -> aesEncryptionProvider.decryptionCipher(encryptedChunk, encryptionMetadata.get())
                );
            }
            if (manifest.compression()) {
                detransformEnum = new DecompressionChunkEnumeration(detransformEnum);
            }
            final DetransformFinisher detransformFinisher = new DetransformFinisher(detransformEnum);
            return detransformFinisher.toInputStream();
        }, executor);
    }
}
