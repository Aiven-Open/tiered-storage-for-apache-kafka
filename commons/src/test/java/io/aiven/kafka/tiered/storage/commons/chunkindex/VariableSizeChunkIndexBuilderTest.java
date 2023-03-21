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

package io.aiven.kafka.tiered.storage.commons.chunkindex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VariableSizeChunkIndexBuilderTest extends ChunkIndexBuilderCommonTest {
    @Override
    @BeforeEach
    protected void init() {
        normalInitedChunkIndexBuilder = new VariableSizeChunkIndexBuilder(100, NON_EMPTY_FILE_SIZE);
        emptyFileChunkIndexBuilder = new VariableSizeChunkIndexBuilder(100, EMPTY_FILE_SIZE);
    }

    @Test
    void incorrectConstructorParams() {
        assertThatThrownBy(() -> new VariableSizeChunkIndexBuilder(-1, 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Original chunk size must be non-negative, -1 given");
        assertThatThrownBy(() -> new VariableSizeChunkIndexBuilder(100, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Original file size must be non-negative, -1 given");
    }

    @Test
    void threeChunks() {
        /*
        Original file:        |  Transformed file:
        [0-101   - size 101)  |  [0-33  - size 33)
        [101-202 - size 101)  |  [33-55 - size 22)
        [202-253 - size 51)   |  [55-60 - size 5)
        */
        final var builder = new VariableSizeChunkIndexBuilder(101, 253);
        builder.addChunk(33);
        builder.addChunk(22);
        final ChunkIndex index = builder.finish(5);

        final var transformedChunk1 = new Chunk(0, 0, 101, 0, 33);
        final var transformedChunk2 = new Chunk(1, 101, 101, 33, 22);
        final var transformedChunk3 = new Chunk(2, 202, 51, 55, 5);

        assertThat(index.chunks()).containsExactly(transformedChunk1, transformedChunk2, transformedChunk3);

        for (int i = 0; i < 101; i++) {
            assertThat(index.findChunkForOriginalOffset(i))
                .isEqualTo(transformedChunk1);
        }

        for (int i = 101; i < 202; i++) {
            assertThat(index.findChunkForOriginalOffset(i))
                .isEqualTo(transformedChunk2);
        }

        for (int i = 202; i < 253; i++) {
            assertThat(index.findChunkForOriginalOffset(i))
                .isEqualTo(transformedChunk3);
        }

        assertThat(index.findChunkForOriginalOffset(253)).isNull();
        assertThat(index.findChunkForOriginalOffset(254)).isNull();
    }
}
