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
import java.util.Optional;

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

public class DefaultFetchManager implements FetchManager {
    private final ObjectFetcher fetcher;
    private final AesEncryptionProvider aesEncryptionProvider;

    public DefaultFetchManager(final ObjectFetcher fetcher, final AesEncryptionProvider aesEncryptionProvider) {
        this.fetcher = fetcher;
        this.aesEncryptionProvider = aesEncryptionProvider;
    }

    /**
     * Gets a part of a segment.
     *
     * @return an {@link InputStream} of the fetch part, plain text (i.e., decrypted and decompressed).
     */
    @Override
    public InputStream fetchPartContent(final ObjectKey objectKey,
                                        final SegmentManifest manifest,
                                        final FetchPart part) throws StorageBackendException {
        final InputStream partContent = fetcher.fetch(objectKey, part.range);

        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(partContent, part.chunks);
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
        return new DetransformFinisher(detransformEnum).toInputStream();
    }
}
