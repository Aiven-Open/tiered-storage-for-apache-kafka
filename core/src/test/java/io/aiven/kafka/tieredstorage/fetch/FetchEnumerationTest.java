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

import java.io.ByteArrayInputStream;
import java.util.NoSuchElementException;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchEnumerationTest {
    @Mock
    DefaultFetchManager chunkManager;

    final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 100, 10, 100);

    final SegmentIndexesV1 segmentIndexesV1 = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();
    final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, segmentIndexesV1, false, null);

    static final byte[] CHUNK_CONTENT = "0123456789".getBytes();
    static final ObjectKey SEGMENT_KEY = new TestObjectKey("topic/segment");

    // Test scenarios
    // - Initialization
    //   - Invalid start position
    @Test
    void failsWhenLargerStartPosition() {
        // Given
        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, segmentIndexesV1, false, null);
        // When
        final int from = 1000;
        final int to = from + 1;
        // Then
        assertThatThrownBy(
            () -> new FetchEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to), 1))
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
        final FetchEnumeration fetchEnumeration =
            new FetchEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to), 1);
        assertThat(fetchEnumeration.firstChunk.id).isEqualTo(0);
        assertThat(fetchEnumeration.lastChunk.id).isEqualTo(8);
    }

    //   - End position outside index
    @Test
    void endPositionIsOutsideIndex() {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 0;
        final int to = 110;
        final FetchEnumeration fetchEnumeration =
            new FetchEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to), 1);
        // Then
        assertThat(fetchEnumeration.firstChunk.id).isEqualTo(0);
        assertThat(fetchEnumeration.lastChunk.id).isEqualTo(9);
    }

    // - Single chunk
    @Test
    void shouldReturnRangeFromSingleChunk() throws StorageBackendException {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 32;
        final int to = 34;
        final FetchEnumeration fetchEnumeration =
            new FetchEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to), 1);
        when(chunkManager.fetchPartContent(eq(SEGMENT_KEY), eq(manifest), any()))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        // Then
        assertThat(fetchEnumeration.firstChunk.id).isEqualTo(fetchEnumeration.lastChunk.id);
        assertThat(fetchEnumeration.nextElement()).hasContent("234");
        assertThat(fetchEnumeration.hasMoreElements()).isFalse();
        assertThatThrownBy(fetchEnumeration::nextElement).isInstanceOf(NoSuchElementException.class);
    }

    // - Multiple chunks
    @Test
    void shouldReturnRangeFromMultipleParts() throws StorageBackendException {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 15;
        final int to = 34;
        final FetchEnumeration fetchEnumeration =
            new FetchEnumeration(chunkManager, SEGMENT_KEY, manifest, BytesRange.of(from, to), 1);
        final var part0 = new FetchPart(chunkIndex, chunkIndex.chunks().get(0), 1);
        final var part1 = part0.next().get();
        when(chunkManager.fetchPartContent(SEGMENT_KEY, manifest, part1))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        final var part2 = part1.next().get();
        when(chunkManager.fetchPartContent(SEGMENT_KEY, manifest, part2))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        final var part3 = part2.next().get();
        when(chunkManager.fetchPartContent(SEGMENT_KEY, manifest, part3))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        // Then
        assertThat(fetchEnumeration.firstChunk.id).isNotEqualTo(fetchEnumeration.lastChunk.id);
        assertThat(fetchEnumeration.nextElement()).hasContent("56789");
        assertThat(fetchEnumeration.nextElement()).hasContent("0123456789");
        assertThat(fetchEnumeration.nextElement()).hasContent("01234");
        assertThat(fetchEnumeration.hasMoreElements()).isFalse();
        assertThatThrownBy(fetchEnumeration::nextElement).isInstanceOf(NoSuchElementException.class);
    }
}
