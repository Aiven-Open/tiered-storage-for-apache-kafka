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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
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
class AbstractChunkCacheTest {

    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "1011121314".getBytes();

    private static final String TEST_EXCEPTION_MESSAGE = "test_message";

    private static final ChunkKey CHUNK_KEY_0 = new ChunkKey("topic/segment", 0);
    private static final ChunkKey CHUNK_KEY_1 = new ChunkKey("topic/segment", 1);

    @Mock
    private Supplier<CompletableFuture<InputStream>> chunk0Supplier;
    @Mock
    private Supplier<CompletableFuture<InputStream>> chunk1Supplier;
    private AbstractChunkCache<?> chunkCache;

    @BeforeEach
    void setUp() {
        chunkCache = spy(new InMemoryChunkCache());
    }

    @AfterEach
    void tearDown() {
        reset(chunk0Supplier);
        reset(chunk1Supplier);
    }

    @Nested
    class CacheTests {
        @Mock
        RemovalListener<ChunkKey, ?> removalListener;

        @BeforeEach
        void setUp() {
            doAnswer(invocation -> removalListener).when(chunkCache).removalListener();

            // The second invocation is needed for some tests.
            when(chunk0Supplier.get())
                .thenAnswer(i -> CompletableFuture.completedFuture(new ByteArrayInputStream(CHUNK_0)));
            when(chunk1Supplier.get())
                .thenAnswer(i -> CompletableFuture.completedFuture(new ByteArrayInputStream(CHUNK_1)));
        }

        @Test
        void noEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                    "retention.ms", "-1",
                    "size", "-1"
            ));

            final InputStream chunk0 = chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier);
            assertThat(chunk0).hasBinaryContent(CHUNK_0);
            verify(chunk0Supplier).get();
            final InputStream cachedChunk0 = chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier);
            assertThat(cachedChunk0).hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunk0Supplier);

            final InputStream chunk1 = chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier);
            assertThat(chunk1).hasBinaryContent(CHUNK_1);
            verify(chunk1Supplier).get();
            final InputStream cachedChunk1 = chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier);
            assertThat(cachedChunk1).hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunk1Supplier);

            verifyNoInteractions(removalListener);
        }

        @Test
        void timeBasedEviction() throws IOException, StorageBackendException, InterruptedException {
            chunkCache.configure(Map.of(
                    "retention.ms", "100",
                    "size", "-1"
            ));

            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            verify(chunk0Supplier).get();
            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunk0Supplier);

            Thread.sleep(100);

            assertThat(chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier))
                    .hasBinaryContent(CHUNK_1);
            verify(chunk1Supplier).get();
            assertThat(chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier))
                    .hasBinaryContent(CHUNK_1);
            verifyNoMoreInteractions(chunk1Supplier);

            await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(100))
                    .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener)
                    .onRemoval(
                            argThat(argument -> argument.chunkId == 0),
                            any(),
                            eq(RemovalCause.EXPIRED));

            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            verify(chunk0Supplier, times(2)).get();
        }

        @Test
        void sizeBasedEviction() throws IOException, StorageBackendException {
            chunkCache.configure(Map.of(
                    "retention.ms", "-1",
                    "size", "18"
            ));

            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            verify(chunk0Supplier).get();
            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            verifyNoMoreInteractions(chunk0Supplier);

            assertThat(chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier))
                    .hasBinaryContent(CHUNK_1);
            verify(chunk1Supplier).get();

            await().atMost(Duration.ofMillis(5000))
                    .pollDelay(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(10))
                    .until(() -> !mockingDetails(removalListener).getInvocations().isEmpty());

            verify(removalListener).onRemoval(any(ChunkKey.class), any(), eq(RemovalCause.SIZE));

            assertThat(chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .hasBinaryContent(CHUNK_0);
            assertThat(chunkCache.getChunk(CHUNK_KEY_1, chunk1Supplier))
                    .hasBinaryContent(CHUNK_1);
            verify(chunk0Supplier, times(1)).get();
            verify(chunk1Supplier, times(2)).get();
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
        void failedSupply() {
            when(chunk0Supplier.get())
                .thenReturn(CompletableFuture.failedFuture(new StorageBackendException(TEST_EXCEPTION_MESSAGE)))
                .thenReturn(CompletableFuture.failedFuture(new IOException(TEST_EXCEPTION_MESSAGE)));

            assertThatThrownBy(() -> chunkCache
                    .getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .isInstanceOf(StorageBackendException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .isInstanceOf(IOException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithInterruptedException() throws Exception {
            when(chunk0Supplier.get())
                .thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(CHUNK_0)));

            doCallRealMethod().doAnswer(invocation -> {
                throw new InterruptedException(TEST_EXCEPTION_MESSAGE);
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(InterruptedException.class)
                    .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }

        @Test
        void failedReadingCachedValueWithExecutionException() throws Exception {
            when(chunk0Supplier.get())
                .thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(CHUNK_0)));
            doCallRealMethod().doAnswer(invocation -> {
                throw new ExecutionException(new RuntimeException(TEST_EXCEPTION_MESSAGE));
            }).when(chunkCache).cachedChunkToInputStream(any());

            chunkCache.getChunk(CHUNK_KEY_0, chunk0Supplier);
            assertThatThrownBy(() -> chunkCache
                    .getChunk(CHUNK_KEY_0, chunk0Supplier))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(RuntimeException.class)
                    .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }
}
