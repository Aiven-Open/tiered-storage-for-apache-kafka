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

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformFinisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultChunkManager implements ChunkManager {
    private static final Logger log = LoggerFactory.getLogger(DefaultChunkManager.class);

    private final ObjectFetcher fetcher;
    private final AesEncryptionProvider aesEncryptionProvider;
    private final boolean useNewMode;

    public DefaultChunkManager(final ObjectFetcher fetcher, final AesEncryptionProvider aesEncryptionProvider,
                               final boolean useNewMode) {
        this.fetcher = fetcher;
        this.aesEncryptionProvider = aesEncryptionProvider;
        this.useNewMode = useNewMode;
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e., decrypted and decompressed).
     */
    public InputStream getChunk(final ObjectKey objectKey, final SegmentManifest manifest,
                                final int chunkId) throws StorageBackendException {
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);
        final InputStream chunkContent = useNewMode
            ? getChunkContentNewMode(objectKey, chunk)
            : getChunkContentOldMode(objectKey, chunk);

        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(chunkContent, List.of(chunk));
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
    }

    private InputStream getChunkContentOldMode(final ObjectKey objectKey,
                                               final Chunk chunk) throws StorageBackendException {
        return fetcher.fetch(objectKey, chunk.range());
    }

    private final PersistentObjectInputStreamCache streamCache = new PersistentObjectInputStreamCache();
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    private InputStream getChunkContentNewMode(final ObjectKey objectKey,
                                               final Chunk chunk) throws StorageBackendException {
        final int requestId = requestCounter.getAndIncrement();
        log.error("[{}] I need chunk: {}", requestId, chunk);

        try (final var streamHandler = streamCache.borrowOrCreate(requestId, objectKey, chunk)) {
            final var stream = streamHandler.inputStream;

            if (chunk.equals(stream.currentChunk)) {
                log.error("[{}] Chunk cached", requestId);
                return new ByteArrayInputStream(stream.currentChunkContent);
            }

            log.error("[{}] Reading chunk content", requestId);
            final byte[] chunkBytes = stream.readChunk(chunk);
            log.error("[{}] Read chunk content", requestId);
            return new ByteArrayInputStream(chunkBytes);
        } catch (final Exception e) {
            throw new StorageBackendException("error", e);
        }
    }

    private static class PersistentObjectInputStream extends FilterInputStream {
        public int position = 0;
        public Chunk currentChunk = null;
        public byte[] currentChunkContent = null;

        private PersistentObjectInputStream(final InputStream in) {
            super(in);
        }

        private byte[] readChunk(final Chunk chunk) throws StorageBackendException {
            if (chunk.transformedPosition != position) {
                throw new IllegalArgumentException("Invalid chunk " + chunk + ", current position: " + position);
            }

            currentChunk = chunk;
            try {
                final int size = chunk.range().size();
                currentChunkContent = in.readNBytes(size);
                if (currentChunkContent.length != size) {
                    throw new StorageBackendException(
                        "Expected " + size + " bytes for chunk " + chunk + " but got " + currentChunkContent.length);
                }
                position += size;
                return currentChunkContent;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class PersistentObjectInputStreamCache {
        private final Object lock = new Object();
        private final HashMap<ObjectKey, List<PersistentObjectInputStream>> persistentStreams = new HashMap<>();

        StreamHandler borrowOrCreate(final int requestId,
                                     final ObjectKey objectKey,
                                     final Chunk chunk) throws StorageBackendException {
            synchronized (lock) {
                final List<PersistentObjectInputStream> streams = persistentStreams.get(objectKey);
                log.error("[{}] Streams now: {}", requestId, persistentStreams);
                final int from = chunk.range().from;
                if (streams == null) {
                    log.error("[{}] Opening new stream", requestId);
                    return new StreamHandler(requestId, objectKey,
                        new PersistentObjectInputStream(fetcher.getContinuousStream(objectKey, from)));
                }

                for (int i = 0; i < streams.size(); i++) {
                    final var stream = streams.get(i);
                    if (stream.currentChunk.equals(chunk)) {
                        log.error("[{}] Stream cached", requestId);
                        streams.remove(i);
                        return new StreamHandler(requestId, objectKey, stream);
                    }
                }

                for (int i = 0; i < streams.size(); i++) {
                    final var stream = streams.get(i);
                    if (stream.position == from) {
                        log.error("[{}] Stream cached", requestId);
                        streams.remove(i);
                        return new StreamHandler(requestId, objectKey, stream);
                    }
                }

                log.error("[{}] Opening new stream", requestId);
                return new StreamHandler(requestId, objectKey,
                    new PersistentObjectInputStream(fetcher.getContinuousStream(objectKey, from)));
            }
        }

        class StreamHandler implements AutoCloseable {
            private final int requestId;
            private final ObjectKey objectKey;
            public final PersistentObjectInputStream inputStream;

            private StreamHandler(final int requestId,
                                  final ObjectKey objectKey,
                                  final PersistentObjectInputStream inputStream) {
                this.requestId = requestId;
                this.objectKey = objectKey;
                this.inputStream = inputStream;
            }

            @Override
            public void close() throws Exception {
                synchronized (lock) {
                    log.error("[{}] Returning stream for {}", requestId, objectKey);
                    final var list = persistentStreams.computeIfAbsent(objectKey, k -> new ArrayList<>());
                    list.add(inputStream);
                    log.error("[{}] Streams now: {}", requestId, persistentStreams);
                }
            }
        }
    }
}
