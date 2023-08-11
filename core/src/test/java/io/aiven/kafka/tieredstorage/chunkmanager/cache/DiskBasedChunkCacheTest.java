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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.Uuid;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCacheConfig.CACHE_DIRECTORY;
import static io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCacheConfig.TEMP_CACHE_DIRECTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DiskBasedChunkCacheTest {
    public static final Uuid SEGMENT_ID = Uuid.randomUuid();
    private static final byte[] CHUNK_0 = "0123456789".getBytes();
    private static final byte[] CHUNK_1 = "1011121314".getBytes();
    private static final String TEST_EXCEPTION_MESSAGE = "test_message";
    @Mock
    ChunkManager chunkManager;
    @TempDir
    Path baseCachePath;

    DiskBasedChunkCache diskBasedChunkCache = new DiskBasedChunkCache(chunkManager);
    private Path cachePath;
    private Path tempCachePath;

    @BeforeEach
    void setUp() {
        diskBasedChunkCache.configure(Map.of(
                "retention.ms", "-1",
                "size", "-1",
                "path", baseCachePath.toString()
        ));
        cachePath = baseCachePath.resolve(CACHE_DIRECTORY);
        tempCachePath = baseCachePath.resolve(TEMP_CACHE_DIRECTORY);
    }

    @Test
    void failedToMoveFromTempCache() {
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.move(any(), any(), any()))
                    .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            final ByteArrayInputStream chunkStream0 = new ByteArrayInputStream(CHUNK_0);
            final ByteArrayInputStream chunkStream1 = new ByteArrayInputStream(CHUNK_1);
            final ChunkKey chunkKey0 = new ChunkKey(SEGMENT_ID, 0);
            final ChunkKey chunkKey1 = new ChunkKey(SEGMENT_ID, 1);

            assertThatThrownBy(() -> diskBasedChunkCache.cacheChunk(chunkKey0, chunkStream0))
                    .isInstanceOf(IOException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);

            assertThatThrownBy(() -> diskBasedChunkCache.cacheChunk(chunkKey1, chunkStream1))
                    .isInstanceOf(IOException.class)
                    .hasMessage(TEST_EXCEPTION_MESSAGE);

            assertThat(cachePath)
                    .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey0.chunkId));
            assertThat(tempCachePath)
                    .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey0.chunkId));

            assertThat(cachePath)
                    .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey1.chunkId));
            assertThat(tempCachePath)
                    .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey1.chunkId));
        }
    }

    @Test
    void cacheChunks() throws IOException {
        final ByteArrayInputStream chunkStream0 = new ByteArrayInputStream(CHUNK_0);
        final ByteArrayInputStream chunkStream1 = new ByteArrayInputStream(CHUNK_1);
        final ChunkKey chunkKey0 = new ChunkKey(SEGMENT_ID, 0);
        final ChunkKey chunkKey1 = new ChunkKey(SEGMENT_ID, 1);

        final Path cachedChunkPath0 = diskBasedChunkCache.cacheChunk(chunkKey0, chunkStream0);
        final Path cachedChunkPath1 = diskBasedChunkCache.cacheChunk(chunkKey1, chunkStream1);

        assertThat(cachedChunkPath0).exists();
        assertThat(diskBasedChunkCache.cachedChunkToInputStream(cachedChunkPath0))
                .hasBinaryContent(CHUNK_0);

        assertThat(tempCachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey0.chunkId));

        assertThat(cachedChunkPath1).exists();
        assertThat(diskBasedChunkCache.cachedChunkToInputStream(cachedChunkPath1))
                .hasBinaryContent(CHUNK_1);

        assertThat(tempCachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-" + chunkKey1.chunkId));
    }

    @Test
    void failsToReadFile() {
        assertThatThrownBy(() -> diskBasedChunkCache.cachedChunkToInputStream(Path.of("does_not_exists")))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void weighsCorrectly() throws IOException {
        final ByteArrayInputStream chunkStream = new ByteArrayInputStream(CHUNK_0);
        final ChunkKey chunkKey = new ChunkKey(SEGMENT_ID, 0);

        final Path cachedChunkPath = diskBasedChunkCache.cacheChunk(chunkKey, chunkStream);

        final Weigher<ChunkKey, Path> weigher = diskBasedChunkCache.weigher();
        assertThat(weigher.weigh(chunkKey, cachedChunkPath)).isEqualTo(CHUNK_0.length);
    }

    @Test
    void weighingTooBigFiles() throws IOException {
        final ByteArrayInputStream chunkStream = new ByteArrayInputStream(CHUNK_0);
        final ChunkKey chunkKey = new ChunkKey(SEGMENT_ID, 0);

        final Path cachedChunkPath = diskBasedChunkCache.cacheChunk(chunkKey, chunkStream);

        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.size(any()))
                    .thenReturn((long) Integer.MAX_VALUE + 1);
            final Weigher<ChunkKey, Path> weigher = diskBasedChunkCache.weigher();
            assertThat(weigher.weigh(chunkKey, cachedChunkPath)).isEqualTo(Integer.MAX_VALUE);
        }
    }

    @Test
    void weighingFails() throws IOException {
        final ByteArrayInputStream chunkStream = new ByteArrayInputStream(CHUNK_0);
        final ChunkKey chunkKey = new ChunkKey(SEGMENT_ID, 0);

        final Path cachedChunkPath = diskBasedChunkCache.cacheChunk(chunkKey, chunkStream);
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.size(any()))
                    .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));
            final Weigher<ChunkKey, Path> weigher = diskBasedChunkCache.weigher();
            assertThatThrownBy(() -> weigher.weigh(chunkKey, cachedChunkPath))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(IOException.class)
                    .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }

    @Test
    void removesCorrectly() throws IOException {
        final ByteArrayInputStream chunkStream = new ByteArrayInputStream(CHUNK_0);
        final ChunkKey chunkKey = new ChunkKey(SEGMENT_ID, 0);

        final Path cachedChunkPath = diskBasedChunkCache.cacheChunk(chunkKey, chunkStream);

        final RemovalListener<ChunkKey, Path> removalListener = diskBasedChunkCache.removalListener();
        removalListener.onRemoval(chunkKey, cachedChunkPath, RemovalCause.SIZE);
        assertThat(cachedChunkPath).doesNotExist();
    }

    @Test
    void removalFails() throws IOException {
        final ByteArrayInputStream chunkStream = new ByteArrayInputStream(CHUNK_0);
        final ChunkKey chunkKey = new ChunkKey(SEGMENT_ID, 0);

        final Path cachedChunkPath = diskBasedChunkCache.cacheChunk(chunkKey, chunkStream);

        final RemovalListener<ChunkKey, Path> removalListener = diskBasedChunkCache.removalListener();
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.delete(any()))
                    .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            assertThatNoException()
                    .isThrownBy(() -> removalListener.onRemoval(chunkKey, cachedChunkPath, RemovalCause.SIZE));
            assertThat(cachedChunkPath).exists();
        }
    }

    @Test
    void cacheInitialized() {
        final DiskBasedChunkCache spy = spy(
                new DiskBasedChunkCache(mock(ChunkManager.class))
        );
        final Map<String, String> configs = Map.of(
                "retention.ms", "-1",
                "size", "-1",
                "path", baseCachePath.toString()
        );
        spy.configure(configs);

        assertThat(spy.cache).isNotNull();
        verify(spy).buildCache(new DiskBasedChunkCacheConfig(configs));
    }
}
