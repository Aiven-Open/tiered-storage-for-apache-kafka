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

package io.aiven.kafka.tieredstorage.commons.index;

import io.aiven.kafka.tieredstorage.commons.Chunk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FixedSizeChunkIndexBuilderTest extends ChunkIndexBuilderCommonTest {
    @Override
    @BeforeEach
    protected void init() {
        normalInitedChunkIndexBuilder = new FixedSizeChunkIndexBuilder(100, NON_EMPTY_FILE_SIZE, 113);
        emptyFileChunkIndexBuilder = new FixedSizeChunkIndexBuilder(100, EMPTY_FILE_SIZE, 113);
    }

    @Test
    void incorrectConstructorParams() {
        assertThatThrownBy(() -> new FixedSizeChunkIndexBuilder(-1, 100, 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Original chunk size must be non-negative, -1 given");
        assertThatThrownBy(() -> new FixedSizeChunkIndexBuilder(100, -1, 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Original file size must be non-negative, -1 given");
        assertThatThrownBy(() -> new FixedSizeChunkIndexBuilder(100, 100, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Transformed chunk size must be non-negative, -1 given");
    }

    @Test
    void addInvalidNonFinalTransformedChunkSize() {
        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.addChunk(12))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Non-final chunk must be of size 113, but 12 given");
    }

    @Test
    void threeChunks() {
        /*
        Original file:        |  Transformed file:
        [0-101   - size 101)  |  [0-110   - size 111)
        [101-202 - size 101)  |  [111-222 - size 111)
        [202-253 - size 51)   |  [222-304 - size 82)
        */
        final var builder = new FixedSizeChunkIndexBuilder(101, 253, 111);
        builder.addChunk(111);
        builder.addChunk(111);
        final ChunkIndex index = builder.finish(81);

        final var transformedChunk1 = new Chunk(0, 0, 101, 0, 111);
        final var transformedChunk2 = new Chunk(1, 101, 101, 111, 111);
        final var transformedChunk3 = new Chunk(2, 202, 51, 222, 81);

        assertThat(index.chunks()).containsExactly(transformedChunk1, transformedChunk2, transformedChunk3);

        for (int i = 0; i < 101; i++) {
            assertThat(index.findChunkForOriginalOffset(i)).isEqualTo(transformedChunk1);
        }

        for (int i = 101; i < 202; i++) {
            assertThat(index.findChunkForOriginalOffset(i)).isEqualTo(transformedChunk2);
        }

        for (int i = 202; i < 253; i++) {
            assertThat(index.findChunkForOriginalOffset(i)).isEqualTo(transformedChunk3);
        }

        assertThat(index.findChunkForOriginalOffset(253)).isNull();
        assertThat(index.findChunkForOriginalOffset(254)).isNull();
    }
}
