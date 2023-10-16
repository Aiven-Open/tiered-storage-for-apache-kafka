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

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.chunkmanager.DefaultChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class FetchChunkEnumerationTest {
    @Mock
    DefaultChunkManager chunkManager;

    final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 100, 10, 100);

    final SegmentIndexesV1 segmentIndexesV1 = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();
    final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, segmentIndexesV1, false, null, null);

    static final byte[] CHUNK_CONTENT = "0123456789".getBytes();
    static final ObjectKey SEGMENT_KEY = new TestObjectKey("topic/segment");

    // Test scenarios
    // - Initialization
    //   - Invalid start position
    @Test
    void failsWhenLargerStartPosition() {
        // Given
        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, segmentIndexesV1, false, null, null);
        // When
        final int from = 1000;
        final int to = from + 1;
        // Then
        assertThatThrownBy(
            () -> new FetchChunkEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to)))
            .hasMessage("Invalid start position " + from + " in segment path topic/segment");
    }

    //   - End position within index
    @Test
    void endPositionIsWithinIndex() {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 0;
        final int to = 80;
        // Then
        final FetchChunkEnumeration fetchChunk =
            new FetchChunkEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to));
        assertThat(fetchChunk.startChunkId).isEqualTo(0);
        assertThat(fetchChunk.lastChunkId).isEqualTo(8);
    }

    //   - End position outside index
    @Test
    void endPositionIsOutsideIndex() {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 0;
        final int to = 110;
        final FetchChunkEnumeration fetchChunk =
            new FetchChunkEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to));
        // Then
        assertThat(fetchChunk.startChunkId).isEqualTo(0);
        assertThat(fetchChunk.lastChunkId).isEqualTo(9);
    }

    // TODO move this logic down to the cache
//    // - Single chunk
//    @Test
//    void shouldReturnRangeFromSingleChunk() throws StorageBackendException, IOException {
//        // Given a set of 10 chunks with 10 bytes each
//        // When
//        final int from = 32;
//        final int to = 34;
//        final FetchChunkEnumeration fetchChunk =
//            new FetchChunkEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to));
//        when(chunkManager.getChunk(SEGMENT_KEY, manifest, fetchChunk.currentChunkId, BytesRange.of(2, 4)))
//            .thenReturn(ByteBuffer.wrap(CHUNK_CONTENT));
//        // Then
//        assertThat(fetchChunk.startChunkId).isEqualTo(fetchChunk.lastChunkId);
//        assertThat(fetchChunk.nextElement()).hasContent("234");
//        assertThat(fetchChunk.hasMoreElements()).isFalse();
//        assertThatThrownBy(fetchChunk::nextElement).isInstanceOf(NoSuchElementException.class);
//    }
//
//    // - Multiple chunks
//    @Test
//    void shouldReturnRangeFromMultipleChunks() throws StorageBackendException, IOException {
//        // Given a set of 10 chunks with 10 bytes each
//        // When
//        final int from = 15;
//        final int to = 34;
//        final FetchChunkEnumeration fetchChunk =
//            new FetchChunkEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to));
//        when(chunkManager.getChunk(SEGMENT_KEY, manifest, 1, BytesRange.of(5, 9)))
//            .thenReturn(ByteBuffer.wrap(CHUNK_CONTENT));
//        when(chunkManager.getChunk(SEGMENT_KEY, manifest, 2, BytesRange.of(0, 9)))
//            .thenReturn(ByteBuffer.wrap(CHUNK_CONTENT));
//        when(chunkManager.getChunk(SEGMENT_KEY, manifest, 3, BytesRange.of(0, 4)))
//            .thenReturn(ByteBuffer.wrap(CHUNK_CONTENT));
//        // Then
//        assertThat(fetchChunk.startChunkId).isNotEqualTo(fetchChunk.lastChunkId);
//        assertThat(fetchChunk.nextElement()).hasContent("56789");
//        assertThat(fetchChunk.nextElement()).hasContent("0123456789");
//        assertThat(fetchChunk.nextElement()).hasContent("01234");
//        assertThat(fetchChunk.hasMoreElements()).isFalse();
//        assertThatThrownBy(fetchChunk::nextElement).isInstanceOf(NoSuchElementException.class);
//    }
}
