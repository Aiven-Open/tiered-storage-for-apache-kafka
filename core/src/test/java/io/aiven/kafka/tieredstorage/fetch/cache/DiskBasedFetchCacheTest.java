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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.fetch.FetchManager;
import io.aiven.kafka.tieredstorage.fetch.FetchPartKey;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

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

import static io.aiven.kafka.tieredstorage.fetch.cache.DiskBasedFetchCacheConfig.CACHE_DIRECTORY;
import static io.aiven.kafka.tieredstorage.fetch.cache.DiskBasedFetchCacheConfig.TEMP_CACHE_DIRECTORY;
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
class DiskBasedFetchCacheTest {
    public static final String SEGMENT_ID = "topic/segment";
    private static final byte[] PART_0 = "0123456789".getBytes();
    private static final byte[] PART_1 = "1011121314".getBytes();
    private static final String TEST_EXCEPTION_MESSAGE = "test_message";
    @Mock
    FetchManager fetchManager;
    @TempDir
    Path baseCachePath;

    DiskBasedFetchCache diskBasedFetchCache = new DiskBasedFetchCache(fetchManager);
    private Path cachePath;
    private Path tempCachePath;

    @BeforeEach
    void setUp() {
        diskBasedFetchCache.configure(Map.of(
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

            final ByteArrayInputStream partStream0 = new ByteArrayInputStream(PART_0);
            final ByteArrayInputStream partStream1 = new ByteArrayInputStream(PART_1);
            final FetchPartKey fetchPartKey0 = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));
            final FetchPartKey fetchPartKey1 = new FetchPartKey(SEGMENT_ID, BytesRange.of(10, 19));

            assertThatThrownBy(() -> diskBasedFetchCache.cachePartContent(fetchPartKey0, partStream0))
                .isInstanceOf(IOException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);

            assertThatThrownBy(() -> diskBasedFetchCache.cachePartContent(fetchPartKey1, partStream1))
                .isInstanceOf(IOException.class)
                .hasMessage(TEST_EXCEPTION_MESSAGE);

            assertThat(cachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                    + fetchPartKey0.range.from + "-" + fetchPartKey0.range.to));
            assertThat(tempCachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                    + fetchPartKey0.range.from + "-" + fetchPartKey0.range.to));

            assertThat(cachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                    + fetchPartKey1.range.from + "-" + fetchPartKey1.range.to));
            assertThat(tempCachePath)
                .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                    + fetchPartKey1.range.from + "-" + fetchPartKey1.range.to));
        }
    }

    @Test
    void cacheParts() throws IOException {
        final ByteArrayInputStream partStream0 = new ByteArrayInputStream(PART_0);
        final ByteArrayInputStream partStream1 = new ByteArrayInputStream(PART_1);
        final FetchPartKey fetchPartKey0 = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));
        final FetchPartKey fetchPartKey1 = new FetchPartKey(SEGMENT_ID, BytesRange.of(10, 19));

        final Path cachedPath0 = diskBasedFetchCache.cachePartContent(fetchPartKey0, partStream0);
        final Path cachedPath1 = diskBasedFetchCache.cachePartContent(fetchPartKey1, partStream1);

        assertThat(cachedPath0).exists();
        assertThat(diskBasedFetchCache.readCachedPartContent(cachedPath0))
            .hasBinaryContent(PART_0);

        assertThat(tempCachePath)
            .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                + fetchPartKey0.range.from + "-" + fetchPartKey0.range.to));

        assertThat(cachedPath1).exists();
        assertThat(diskBasedFetchCache.readCachedPartContent(cachedPath1))
            .hasBinaryContent(PART_1);

        assertThat(tempCachePath)
            .isDirectoryNotContaining(path -> path.endsWith(SEGMENT_ID + "-"
                + fetchPartKey1.range.from + "-" + fetchPartKey1.range.to));
    }

    @Test
    void failsToReadFile() {
        assertThatThrownBy(() -> diskBasedFetchCache.readCachedPartContent(Path.of("does_not_exists")))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void weighsCorrectly() throws IOException {
        final ByteArrayInputStream partStream = new ByteArrayInputStream(PART_0);
        final FetchPartKey fetchPartKey = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));

        final Path cachedPath = diskBasedFetchCache.cachePartContent(fetchPartKey, partStream);

        final Weigher<FetchPartKey, Path> weigher = diskBasedFetchCache.weigher();
        assertThat(weigher.weigh(fetchPartKey, cachedPath)).isEqualTo(PART_0.length);
    }

    @Test
    void weighingTooBigFiles() throws IOException {
        final ByteArrayInputStream partStream = new ByteArrayInputStream(PART_0);
        final FetchPartKey fetchPartKey = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));

        final Path cachedPath = diskBasedFetchCache.cachePartContent(fetchPartKey, partStream);

        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.size(any()))
                .thenReturn((long) Integer.MAX_VALUE + 1);
            final Weigher<FetchPartKey, Path> weigher = diskBasedFetchCache.weigher();
            assertThat(weigher.weigh(fetchPartKey, cachedPath)).isEqualTo(Integer.MAX_VALUE);
        }
    }

    @Test
    void weighingFails() throws IOException {
        final ByteArrayInputStream partStream = new ByteArrayInputStream(PART_0);
        final FetchPartKey fetchPartKey = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));

        final Path cachedPath = diskBasedFetchCache.cachePartContent(fetchPartKey, partStream);
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.size(any()))
                .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));
            final Weigher<FetchPartKey, Path> weigher = diskBasedFetchCache.weigher();
            assertThatThrownBy(() -> weigher.weigh(fetchPartKey, cachedPath))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage(TEST_EXCEPTION_MESSAGE);
        }
    }

    @Test
    void removesCorrectly() throws IOException {
        final ByteArrayInputStream partStream = new ByteArrayInputStream(PART_0);
        final FetchPartKey fetchPartKey = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));

        final Path cachedPath = diskBasedFetchCache.cachePartContent(fetchPartKey, partStream);

        final RemovalListener<FetchPartKey, Path> removalListener = diskBasedFetchCache.removalListener();
        removalListener.onRemoval(fetchPartKey, cachedPath, RemovalCause.SIZE);
        assertThat(cachedPath).doesNotExist();
    }

    @Test
    void removalFails() throws IOException {
        final ByteArrayInputStream partStream = new ByteArrayInputStream(PART_0);
        final FetchPartKey fetchPartKey = new FetchPartKey(SEGMENT_ID, BytesRange.of(0, 9));

        final Path cachedPath = diskBasedFetchCache.cachePartContent(fetchPartKey, partStream);

        final RemovalListener<FetchPartKey, Path> removalListener = diskBasedFetchCache.removalListener();
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> Files.delete(any()))
                .thenThrow(new IOException(TEST_EXCEPTION_MESSAGE));

            assertThatNoException()
                .isThrownBy(() -> removalListener.onRemoval(fetchPartKey, cachedPath, RemovalCause.SIZE));
            assertThat(cachedPath).exists();
        }
    }

    @Test
    void cacheInitialized() {
        final DiskBasedFetchCache spy = spy(
            new DiskBasedFetchCache(mock(FetchManager.class))
        );
        final Map<String, String> configs = Map.of(
            "retention.ms", "-1",
            "size", "-1",
            "path", baseCachePath.toString()
        );
        spy.configure(configs);

        assertThat(spy.cache).isNotNull();
        verify(spy).buildCache(new DiskBasedFetchCacheConfig(configs));
    }
}
