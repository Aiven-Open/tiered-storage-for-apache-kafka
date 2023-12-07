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

package io.aiven.kafka.tieredstorage.fetch;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

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

public class DefaultChunkManager implements ChunkManager {
    private final ObjectFetcher fetcher;
    private final AesEncryptionProvider aesEncryptionProvider;

    public DefaultChunkManager(final ObjectFetcher fetcher, final AesEncryptionProvider aesEncryptionProvider) {
        this.fetcher = fetcher;
        this.aesEncryptionProvider = aesEncryptionProvider;
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e., decrypted and decompressed).
     */
    public InputStream getChunk(final ObjectKey objectKey, final SegmentManifest manifest,
                                final int chunkId) throws StorageBackendException {
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);

        final InputStream chunkContent = fetcher.fetch(objectKey, chunk.range());

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
}
