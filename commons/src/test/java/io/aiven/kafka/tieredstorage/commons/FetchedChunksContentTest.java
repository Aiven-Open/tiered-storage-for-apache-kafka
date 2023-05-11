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

package io.aiven.kafka.tieredstorage.commons;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.ChunkedLogSegmentManager.FetchedChunksContent;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.commons.manifest.index.FixedSizeChunkIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchedChunksContentTest {
    @Mock
    ChunkedLogSegmentManager chunkManager;
    @Mock
    RemoteLogSegmentMetadata remoteLogSegmentMetadata;

    final FixedSizeChunkIndex chunkIndex = new FixedSizeChunkIndex(10, 100, 10, 100);
    final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, false, null);

    static final byte[] CHUNK_CONTENT = "0123456789".getBytes();

    // Test scenarios
    // - Initialization
    //   - Invalid start position
    @Test
    void failsWhenLargerStartPosition() {
        // Given
        final SegmentManifest manifest = new SegmentManifestV1(chunkIndex, false, null);
        // When
        final int from = 1000;
        final int to = from + 1;
        // Then
        assertThatThrownBy(
            () -> new FetchedChunksContent(chunkManager, remoteLogSegmentMetadata, manifest, from, to))
            .hasMessage("Invalid start position " + from + " in segment remoteLogSegmentMetadata");
    }

    //   - End position within index
    @Test
    void endPositionIsWithinIndex() {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 0;
        final int to = 80;
        // Then
        final FetchedChunksContent fetchChunk =
            new FetchedChunksContent(chunkManager, remoteLogSegmentMetadata, manifest, from, to);
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
        final FetchedChunksContent fetchChunk =
            new FetchedChunksContent(chunkManager, remoteLogSegmentMetadata, manifest, from, to);
        // Then
        assertThat(fetchChunk.startChunkId).isEqualTo(0);
        assertThat(fetchChunk.lastChunkId).isEqualTo(9);
    }

    // - Single chunk
    @Test
    void shouldReturnRangeFromSingleChunk() throws IOException {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 32;
        final int to = 35;
        final FetchedChunksContent fetchChunk =
            new FetchedChunksContent(chunkManager, remoteLogSegmentMetadata, manifest, from, to);
        when(chunkManager.fetchChunk(remoteLogSegmentMetadata, manifest, fetchChunk.currentChunkId))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        // Then
        assertThat(fetchChunk.startChunkId).isEqualTo(fetchChunk.lastChunkId);
        assertThat(fetchChunk.nextElement()).hasContent("234");
        assertThat(fetchChunk.hasMoreElements()).isFalse();
        assertThatThrownBy(fetchChunk::nextElement).isInstanceOf(NoSuchElementException.class);
    }

    // - Multiple chunks
    @Test
    void shouldReturnRangeFromMultipleChunks() throws IOException {
        // Given a set of 10 chunks with 10 bytes each
        // When
        final int from = 15;
        final int to = 35;
        final FetchedChunksContent fetchChunk =
            new FetchedChunksContent(chunkManager, remoteLogSegmentMetadata, manifest, from, to);
        when(chunkManager.fetchChunk(remoteLogSegmentMetadata, manifest, 1))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        when(chunkManager.fetchChunk(remoteLogSegmentMetadata, manifest, 2))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        when(chunkManager.fetchChunk(remoteLogSegmentMetadata, manifest, 3))
            .thenReturn(new ByteArrayInputStream(CHUNK_CONTENT));
        // Then
        assertThat(fetchChunk.startChunkId).isNotEqualTo(fetchChunk.lastChunkId);
        assertThat(fetchChunk.nextElement()).hasContent("56789");
        assertThat(fetchChunk.nextElement()).hasContent("0123456789");
        assertThat(fetchChunk.nextElement()).hasContent("01234");
        assertThat(fetchChunk.hasMoreElements()).isFalse();
        assertThatThrownBy(fetchChunk::nextElement).isInstanceOf(NoSuchElementException.class);
    }
}
