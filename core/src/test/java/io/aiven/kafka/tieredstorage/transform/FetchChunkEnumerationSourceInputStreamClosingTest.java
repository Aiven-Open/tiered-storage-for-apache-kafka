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

package io.aiven.kafka.tieredstorage.transform;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManagerFactory;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCache;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FetchChunkEnumerationSourceInputStreamClosingTest {
    static final ObjectKey OBJECT_KEY = () -> "topic/segment";
    static final int CHUNK_SIZE = 10;

    static final BytesRange RANGE1 = BytesRange.ofFromPositionAndSize(0, CHUNK_SIZE);
    static final byte[] DATA1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    static final BytesRange RANGE2 = BytesRange.ofFromPositionAndSize(10, CHUNK_SIZE);
    static final byte[] DATA2 = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    static final BytesRange RANGE3 = BytesRange.ofFromPositionAndSize(20, CHUNK_SIZE);
    static final byte[] DATA3 = {20, 21, 22, 23, 24, 25, 26, 27, 28, 29};

    static final FixedSizeChunkIndex CHUNK_INDEX = new FixedSizeChunkIndex(
        CHUNK_SIZE, CHUNK_SIZE * 3, CHUNK_SIZE, CHUNK_SIZE);
    static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();
    static final SegmentManifest SEGMENT_MANIFEST = new SegmentManifestV1(
        CHUNK_INDEX, SEGMENT_INDEXES, false, null);

    TestObjectFetcher fetcher;

    @BeforeEach
    void setup() {
        fetcher = new TestObjectFetcher();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void test(final Map<String, String> config,
              final boolean readFully,
              final BytesRange range) throws StorageBackendException, IOException {
        final ChunkManagerFactory chunkManagerFactory = new ChunkManagerFactory();
        chunkManagerFactory.configure(config);
        final ChunkManager chunkManager = chunkManagerFactory.initChunkManager(fetcher, null);
        final var is = new FetchChunkEnumeration(chunkManager, OBJECT_KEY, SEGMENT_MANIFEST, range, 1)
            .toInputStream();
        if (readFully) {
            is.readAllBytes();
        } else {
            is.read();
        }
        is.close();
        fetcher.assertAllStreamsWereClosed(readFully);
    }

    static List<Arguments> testParams() throws IOException {
        final BytesRange smallRange = BytesRange.ofFromPositionAndSize(3, 5);
        final BytesRange bigRange = BytesRange.ofFromPositionAndSize(3, 25);

        final List<Arguments> result = new ArrayList<>();
        for (final var readFully : List.of(Named.of("read fully", true), Named.of("read partially", false))) {
            for (final BytesRange range : List.of(smallRange, bigRange)) {
                result.add(Arguments.of(
                    Named.of("without cache", Map.of()),
                    readFully,
                    range)
                );
                result.add(Arguments.of(
                    Named.of("with in-memory cache",
                        Map.of(
                            "chunk.cache.class", InMemoryChunkCache.class.getCanonicalName(),
                            "chunk.cache.size", "-1"
                        )
                    ),
                    readFully,
                    range)
                );
                result.add(Arguments.of(
                    Named.of("with disk-based cache",
                        Map.of(
                            "chunk.cache.class", DiskBasedChunkCache.class.getCanonicalName(),
                            "chunk.cache.path", Files.createTempDirectory("cache").toString(),
                            "chunk.cache.size", "-1"
                        )
                    ),
                    readFully,
                    range)
                );
            }
        }
        return result;
    }

    private static class TestObjectFetcher implements ObjectFetcher {
        private final List<InputStream> openInputStreams = new ArrayList<>();

        @Override
        public InputStream fetch(final ObjectKey key) throws StorageBackendException {
            throw new RuntimeException("Should not be called");
        }

        @Override
        public InputStream fetch(final ObjectKey key, final BytesRange range) {
            if (!key.equals(OBJECT_KEY)) {
                throw new IllegalArgumentException("Invalid key: " + key);
            }

            final InputStream is;
            if (range.equals(RANGE1)) {
                is = spy(new ByteArrayInputStream(DATA1));
            } else if (range.equals(RANGE2)) {
                is = spy(new ByteArrayInputStream(DATA2));
            } else if (range.equals(RANGE3)) {
                is = spy(new ByteArrayInputStream(DATA3));
            } else {
                throw new IllegalArgumentException("Invalid range: " + range);
            }
            openInputStreams.add(is);
            return is;
        }

        public void assertAllStreamsWereClosed(final boolean readFully) throws IOException {
            if (readFully) {
                for (final var is : openInputStreams) {
                    verify(is).close();
                }
            } else {
                assertThat(openInputStreams).hasSize(1);
                verify(openInputStreams.get(0)).close();
            }
        }
    }
}
