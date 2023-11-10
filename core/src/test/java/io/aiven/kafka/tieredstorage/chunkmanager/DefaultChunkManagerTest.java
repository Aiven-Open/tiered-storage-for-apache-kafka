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
import java.io.InputStream;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.AesKeyAwareTest;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.NoOpChunkCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.luben.zstd.ZstdCompressCtx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null, new NoOpChunkCache(), 0);
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
        final ChunkManager chunkManager = new DefaultChunkManager(
            storage, aesEncryptionProvider, new NoOpChunkCache(), 0);

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
        final ChunkManager chunkManager = new DefaultChunkManager(storage, null, new NoOpChunkCache(), 0);

        assertThat(chunkManager.getChunk(OBJECT_KEY, manifest, 0)).hasBinaryContent(TEST_CHUNK_CONTENT);
        verify(storage).fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
    }

    @Nested
    class PrefetchTests {
        private final byte[] chunk0 = "0000000000".getBytes();
        private final byte[] chunk1 = "1111111111".getBytes();
        private final byte[] chunk2 = "2222222222".getBytes();
        private final int chunkSize = chunk0.length;
        private final int fileSize = chunkSize * 3;

        private final FixedSizeChunkIndex chunkIndex =
            new FixedSizeChunkIndex(chunkSize, fileSize, chunkSize, chunkSize);
        private final SegmentManifest manifest =
            new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, false, null, null);

        InMemoryChunkCache chunkCache;

        @BeforeEach
        void setUp() throws StorageBackendException {
            when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range()))
                .thenAnswer(i -> new ByteArrayInputStream(chunk0));
            when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(1).range()))
                .thenAnswer(i -> new ByteArrayInputStream(chunk1));
            when(storage.fetch(OBJECT_KEY, chunkIndex.chunks().get(2).range()))
                .thenAnswer(i -> new ByteArrayInputStream(chunk2));

            chunkCache = spy(new InMemoryChunkCache());
            chunkCache.configure(Map.of("size", "1000"));
        }

        @Test
        void nextChunk() throws Exception {
            final var chunkManager = new DefaultChunkManager(storage, null, chunkCache, chunkSize);

            chunkManager.getChunk(OBJECT_KEY, manifest, 0);

            verify(chunkCache, timeout(10000).times(1))
                .supplyIfAbsent(eq(new ChunkKey(OBJECT_KEY.value(), 1)), any());

            verify(storage, description("first chunk was fetched from remote"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
            verify(storage, description("second chunk was prefetched"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(1).range());
            verify(storage, never().description("third chunk was not prefetched "))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(2).range());
            verifyNoMoreInteractions(storage);

            final InputStream cachedChunk0 = chunkManager.getChunk(OBJECT_KEY, manifest, 0);
            assertThat(cachedChunk0).hasBinaryContent(chunk0);
            verifyNoMoreInteractions(storage);

            // checking that third chunk is prefetch when fetching chunk 1
            final InputStream cachedChunk1 = chunkManager.getChunk(OBJECT_KEY, manifest, 1);
            assertThat(cachedChunk1).hasBinaryContent(chunk1);

            verify(chunkCache, timeout(10000).times(1))
                .supplyIfAbsent(eq(new ChunkKey(OBJECT_KEY.value(), 2)), any());

            verify(storage, description("third chunk was prefetched"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(2).range());
            verifyNoMoreInteractions(storage);
        }

        @Test
        void prefetchingWholeSegment() throws Exception {
            final var chunkManager =
                new DefaultChunkManager(storage, null, chunkCache, fileSize - chunkSize);

            chunkManager.getChunk(OBJECT_KEY, manifest, 0);

            verify(chunkCache, timeout(10000).times(1))
                .supplyIfAbsent(eq(new ChunkKey(OBJECT_KEY.value(), 1)), any());
            verify(chunkCache, timeout(10000).times(1))
                .supplyIfAbsent(eq(new ChunkKey(OBJECT_KEY.value(), 2)), any());

            verify(storage, description("first chunk was fetched from remote"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(0).range());
            verify(storage, description("second chunk was prefetched"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(1).range());
            verify(storage, description("third chunk was prefetched"))
                .fetch(OBJECT_KEY, chunkIndex.chunks().get(2).range());
            verifyNoMoreInteractions(storage);

            // no fetching from remote since chunk 0 is cached
            final InputStream cachedChunk0 = chunkManager.getChunk(OBJECT_KEY, manifest, 0);
            assertThat(cachedChunk0).hasBinaryContent(chunk0);
            verifyNoMoreInteractions(storage);

            // no fetching from remote since chunk 1 is cached
            final InputStream cachedChunk1 = chunkManager.getChunk(OBJECT_KEY, manifest, 1);
            assertThat(cachedChunk1).hasBinaryContent(chunk1);
            verifyNoMoreInteractions(storage);

            // no fetching from remote since chunk 2 is cached
            final InputStream cachedChunk2 = chunkManager.getChunk(OBJECT_KEY, manifest, 2);
            assertThat(cachedChunk2).hasBinaryContent(chunk2);
            verifyNoMoreInteractions(storage);
        }
    }
}
