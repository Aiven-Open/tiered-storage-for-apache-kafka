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

package io.aiven.kafka.tieredstorage.commons;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.cache.UnboundInMemoryChunkCache;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;
import io.aiven.kafka.tieredstorage.commons.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DecompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.DetransformFinisher;

import org.apache.commons.io.IOUtils;

public class ChunkManager {
    private final FileFetcher fileFetcher;
    private final ObjectKey objectKey;
    private final AesEncryptionProvider aesEncryptionProvider;
    private final UnboundInMemoryChunkCache chunkCache;

    public ChunkManager(final FileFetcher fileFetcher, final ObjectKey objectKey,
                        final AesEncryptionProvider aesEncryptionProvider) {
        this.fileFetcher = fileFetcher;
        this.objectKey = objectKey;
        this.aesEncryptionProvider = aesEncryptionProvider;
        this.chunkCache = new UnboundInMemoryChunkCache();
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e. decrypted and decompressed).
     */
    public InputStream getChunk(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final SegmentManifest manifest,
            final int chunkId) throws StorageBackEndException {
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);
        final InputStream chunkContent = getChunkContent(remoteLogSegmentMetadata, chunk, chunkId);
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
        return detransformFinisher.nextElement();
    }

    private InputStream getChunkContent(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final Chunk chunk,
                                       final int chunkId) throws IOException {
        final ChunkKey chunkKey = new ChunkKey(remoteLogSegmentMetadata.remoteLogSegmentId().id(), chunkId);
        final Optional<InputStream> inputStream = chunkCache.get(chunkKey);
        if (inputStream.isEmpty()) {
            final String segmentKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
            final InputStream chunkContent = fileFetcher.fetch(segmentKey, chunk.range());
            final byte[] contentBytes = IOUtils.toByteArray(chunkContent);
            final String tempFilename = chunkCache.storeTemporarily(contentBytes);
            chunkCache.store(tempFilename, chunkKey);
            return new ByteArrayInputStream(contentBytes);
        } else {
            return inputStream.get();
        }
    }
}
