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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChunkCacheTest {

    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "1011121314".getBytes();
    private static final FixedSizeChunkIndex FIXED_SIZE_CHUNK_INDEX = new FixedSizeChunkIndex(10, 20, 10, 10);
    private static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();

    private static final SegmentManifest SEGMENT_MANIFEST =
        new SegmentManifestV1(FIXED_SIZE_CHUNK_INDEX, SEGMENT_INDEXES, false, null, null);
    private static final String TEST_EXCEPTION_MESSAGE = "test_message";
    private static final String SEGMENT_KEY = "topic/segment";
    private static final ObjectKey SEGMENT_OBJECT_KEY = () -> SEGMENT_KEY;

    @Mock
    private ChunkManager chunkManager;
    private ChunkCache<?> chunkCache;

    @BeforeEach
    void setUp() {
        chunkCache = spy(new InMemoryChunkCache(chunkManager));
    }

    @AfterEach
    void tearDown() {
        reset(chunkManager);
    }

    @Nested
    class CacheTests {
        @Mock
        RemovalListener<ChunkKey, ?> removalListener;

        @BeforeEach
        void setUp() throws Exception {
            doAnswer(invocation -> removalListener).when(chunkCache).removalListener();
            when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_0));
            when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .thenAnswer(invocation -> new ByteArrayInputStream(CHUNK_1));
        }

        @Test
        void noEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                    "retention.ms", "-1",
                    "size", "-1"
            ));

            final InputStream chunk0 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(chunk0).hasBinaryContent(CHUNK_0);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            final InputStream cachedChunk0 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(cachedChunk0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            final InputStream chunk1 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            assertThat(chunk1).hasBinaryContent(CHUNK_1);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            final InputStream cachedChunk1 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            assertThat(cachedChunk1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunkManager);

            verifyNoInteractions(removalListener);
        }

        @Test
        void timeBasedEviction() throws IOException, StorageBackendException, InterruptedException {
            chunkCache.configure(Map.of(
                    "retention.ms", "100",
                    "size", "-1"
            ));

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            Thread.sleep(100);

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .hasBinaryContent(CHUNK_1);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunkManager);

            await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
                    .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener)
                    .onRemoval(
                            argThat(argument -> argument.chunkId == 0),
                            any(),
                            eq(RemovalCause.EXPIRED));

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            verify(chunkManager, times(2)).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
        }

        @Test
        void sizeBasedEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                    "retention.ms", "-1",
                    "size", "18"
            ));

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .hasBinaryContent(CHUNK_1);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);

            await().atMost(Duration.ofMillis(5000))
                    .pollDelay(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(10))
                    .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener).onRemoval(any(ChunkKey.class), any(), eq(RemovalCause.SIZE));

            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .hasBinaryContent(CHUNK_0);
            assertThat(chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .hasBinaryContent(CHUNK_1);
            verify(chunkManager, times(3)).getChunk(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), anyInt());
        }

        @Test
        void prefetching() throws Exception {
            chunkCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1",
                "prefetching.bytes", "20" // both chunks
            ));
            chunkCache.startPrefetching(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            await().pollInterval(Duration.ofMillis(5)).until(() -> chunkCache.statsCounter.snapshot().loadCount() == 2);
            verify(chunkManager, times(2)).getChunk(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), anyInt());

            final InputStream cachedChunk0 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(cachedChunk0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            final InputStream cachedChunk1 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            assertThat(cachedChunk1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunkManager);
        }

        @Test
        void prefetchingFirstChunk() throws Exception {
            chunkCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1",
                "prefetching.bytes", "10" // both chunks
            ));
            chunkCache.startPrefetching(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            await().pollInterval(Duration.ofMillis(5)).until(() -> chunkCache.statsCounter.snapshot().loadCount() == 1);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);

            final InputStream cachedChunk0 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThat(cachedChunk0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunkManager);

            final InputStream cachedChunk1 = chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
            assertThat(cachedChunk1).hasBinaryContent(CHUNK_1);
            verify(chunkManager).getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1);
        }
    }

    @Nested
    class ErrorHandlingTests {
        private final Map<String, String> configs = Map.of(
                "retention.ms", "-1",
                "size", "-1"
        );

        @BeforeEach
        void setUp() {
            chunkCache.configure(configs);
        }

        @Test
        void failedFetching() throws Exception {
            when(chunkManager.getChunk(eq(SEGMENT_OBJECT_KEY), eq(SEGMENT_MANIFEST), anyInt()))
                    .thenThrow(new StorageBackendException(TEST_EXCEPTION_MESSAGE))
                    .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            assertThatThrownBy(() -> chunkCache
                    .getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .isInstanceOf(StorageBackendException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 1))
                    .isInstanceOf(IOException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithInterruptedException() throws Exception {
            when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .thenReturn(new ByteArrayInputStream(CHUNK_0));

            doCallRealMethod().doAnswer(invocation -> {
                throw new InterruptedException(TEST_EXCEPTION_MESSAGE);
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(InterruptedException.class)
                    .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithExecutionException() throws Exception {
            when(chunkManager.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0)).thenReturn(
                    new ByteArrayInputStream(CHUNK_0));
            doCallRealMethod().doAnswer(invocation -> {
                throw new ExecutionException(new RuntimeException(TEST_EXCEPTION_MESSAGE));
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(SEGMENT_OBJECT_KEY, SEGMENT_MANIFEST, 0))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(RuntimeException.class)
                    .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }
}
