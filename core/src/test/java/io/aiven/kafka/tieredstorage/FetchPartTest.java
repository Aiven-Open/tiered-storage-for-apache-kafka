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

package io.aiven.kafka.tieredstorage;

import java.util.List;

import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchPartTest {

    @Test
    void invalid() {
        assertThatThrownBy(() -> new FetchPart(null, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Chunk index cannot be null");

        final var emptyChunkIndex = mock(ChunkIndex.class);
        when(emptyChunkIndex.chunks()).thenReturn(List.of());
        assertThatThrownBy(() -> new FetchPart(emptyChunkIndex, null, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Chunk index must contain at least one chunk");

        final var chunkIndex = mock(ChunkIndex.class);
        final var chunk = mock(Chunk.class);
        when(chunkIndex.chunks()).thenReturn(List.of(chunk));
        assertThatThrownBy(() -> new FetchPart(chunkIndex, null, 0))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Chunk cannot be null");

        assertThatThrownBy(() -> new FetchPart(chunkIndex, chunk, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Part size must be higher than zero.");
    }

    @Test
    void minimal() {
        final var chunk = mock(Chunk.class);
        when(chunk.range()).thenReturn(BytesRange.of(0, 9));
        final var chunkIndex = mock(ChunkIndex.class);
        when(chunkIndex.chunks()).thenReturn(List.of(chunk));

        final var part = new FetchPart(chunkIndex, chunk, 1);

        assertThat(part.firstChunkId).isEqualTo(0);
        assertThat(part.chunks).containsExactly(chunk);
        assertThat(part.range).isEqualTo(BytesRange.of(0, 9));
        assertThat(part.finalChunkId).isEqualTo(0);
        assertThat(part.lastChunkId).isEqualTo(0);
    }

    @Test
    void firstAndLastChunkBoundToFinalChunk() {
        final var first = new Chunk(0, 0, 9, 0, 9);
        final var last = new Chunk(1, 10, 19, 10, 19);
        final var chunkIndex = mock(ChunkIndex.class);
        when(chunkIndex.chunks()).thenReturn(List.of(first, last));

        final var part = new FetchPart(chunkIndex, first, 10);

        assertThat(part.firstChunkId).isEqualTo(0);
        assertThat(part.lastChunkId).isEqualTo(1);
    }

    @Test
    void chunksOnSamePartReturnSamePart() {
        final var first = new Chunk(0, 0, 9, 0, 9);
        final var last = new Chunk(1, 10, 19, 10, 19);
        final var chunkIndex = mock(ChunkIndex.class);
        when(chunkIndex.chunks()).thenReturn(List.of(first, last));

        final var part = new FetchPart(chunkIndex, first, 2);
        final var another = new FetchPart(chunkIndex, last, 2);

        assertThat(part).isEqualTo(another);
    }

    @Test
    void lastPartIsEmpty() {
        final var first = new Chunk(0, 0, 9, 0, 9);
        final var last = new Chunk(1, 10, 19, 10, 19);
        final var chunkIndex = mock(ChunkIndex.class);
        when(chunkIndex.chunks()).thenReturn(List.of(first, last));

        final var part = new FetchPart(chunkIndex, first, 2);

        assertThat(part.next()).isEmpty();
    }


    @Test
    void nextPart() {
        final var first = new Chunk(0, 0, 9, 0, 9);
        final var last = new Chunk(1, 10, 19, 10, 19);
        final var chunkIndex = mock(ChunkIndex.class);
        when(chunkIndex.chunks()).thenReturn(List.of(first, last));

        final var part = new FetchPart(chunkIndex, first, 1);
        assertThat(part.firstChunkId).isEqualTo(0);
        assertThat(part.range).isEqualTo(BytesRange.of(0, 8));

        final var next = part.next();
        assertThat(next).isNotEmpty();
        assertThat(next.get().firstChunkId).isEqualTo(1);
        assertThat(next.get().range).isEqualTo(BytesRange.of(10, 28));
    }
}
