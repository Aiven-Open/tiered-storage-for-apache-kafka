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

import javax.crypto.Cipher;

import java.io.ByteArrayInputStream;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.AesKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import com.github.luben.zstd.ZstdCompressCtx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultChunkManagerTest extends AesKeyAwareTest {

    static final ObjectKey OBJECT_KEY = () -> "topic/segment.log";
    static final byte[] TEST_CHUNK_CONTENT = "0123456789".getBytes();
    static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();
    @Mock
    private StorageBackend storage;

    @Test
    void testGetChunk() throws Exception {
        final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 10, 10, 10);

        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, false, null, null);
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null);
        when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range()))
            .thenReturn(new ByteArrayInputStream("0123456789".getBytes()));

        assertThat(chunkManager.getChunk(OBJECT_KEY, manifest, 0)).hasContent("0123456789");
        verify(storage).fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
    }

    @Test
    void testGetChunkWithEncryption() throws Exception {
        final AesEncryptionProvider aesEncryptionProvider = new AesEncryptionProvider();

        final DataKeyAndAAD dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
        final Cipher encryptionCipher = aesEncryptionProvider.encryptionCipher(dataKeyAndAAD);
        final byte[] iv = encryptionCipher.getIV();
        final byte[] encrypted = new byte[iv.length + encryptionCipher.getOutputSize(TEST_CHUNK_CONTENT.length)];
        System.arraycopy(iv, 0, encrypted, 0, iv.length);
        encryptionCipher.doFinal(TEST_CHUNK_CONTENT, 0, TEST_CHUNK_CONTENT.length, encrypted, iv.length);

        final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 10, encrypted.length, encrypted.length);

        when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range())).thenReturn(
            new ByteArrayInputStream(encrypted));

        final var encryption = new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad);
        final var manifest = new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, false, encryption, null);
        final ChunkManager chunkManager = new DefaultChunkManager(storage, aesEncryptionProvider);

        assertThat(chunkManager.getChunk(OBJECT_KEY, manifest, 0)).hasBinaryContent(TEST_CHUNK_CONTENT);
        verify(storage).fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
    }

    @Test
    void testGetChunkWithCompression() throws Exception {

        final byte[] compressed;
        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            compressCtx.setContentSize(true);
            compressed = compressCtx.compress(TEST_CHUNK_CONTENT);
        }
        final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 10, compressed.length, compressed.length);

        when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range()))
            .thenReturn(new ByteArrayInputStream(compressed));

        final var manifest = new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, true, null, null);
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null);

        assertThat(chunkManager.getChunk(OBJECT_KEY, manifest, 0)).hasBinaryContent(TEST_CHUNK_CONTENT);
        verify(storage).fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
    }
}
