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

import javax.crypto.Cipher;

import java.io.ByteArrayInputStream;

import io.aiven.kafka.tieredstorage.AesKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
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

    static final String OBJECT_KEY_PATH = "topic/segment.log";
    static final byte[] TEST_CHUNK_CONTENT = "0123456789".getBytes();
    @Mock
    private StorageBackend storage;

    @Test
    void testGetChunk() throws Exception {
        final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 10, 10, 10);

        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, false, null);
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null);
        when(storage.fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range()))
                .thenReturn(new ByteArrayInputStream("0123456789".getBytes()));

        assertThat(chunkManager.getChunk(OBJECT_KEY_PATH, manifest, 0)).hasContent("0123456789");
        verify(storage).fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range());
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

        when(storage.fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range())).thenReturn(
            new ByteArrayInputStream(encrypted));

        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, false,
            new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad));
        final ChunkManager chunkManager = new DefaultChunkManager(storage, aesEncryptionProvider);

        assertThat(chunkManager.getChunk(OBJECT_KEY_PATH, manifest, 0)).hasBinaryContent(TEST_CHUNK_CONTENT);
        verify(storage).fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range());
    }

    @Test
    void testGetChunkWithCompression() throws Exception {

        final byte[] compressed;
        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            compressCtx.setContentSize(true);
            compressed = compressCtx.compress(TEST_CHUNK_CONTENT);
        }
        final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 10, compressed.length, compressed.length);

        when(storage.fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range()))
                .thenReturn(new ByteArrayInputStream(compressed));

        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, true, null);
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null);

        assertThat(chunkManager.getChunk(OBJECT_KEY_PATH, manifest, 0)).hasBinaryContent(TEST_CHUNK_CONTENT);
        verify(storage).fetch(OBJECT_KEY_PATH, chunkIndex.chunks().get(0).range());
    }
}
