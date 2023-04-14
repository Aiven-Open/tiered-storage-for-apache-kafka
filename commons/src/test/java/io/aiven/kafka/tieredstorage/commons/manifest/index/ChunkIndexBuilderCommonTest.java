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

package io.aiven.kafka.tieredstorage.commons.manifest.index;

import io.aiven.kafka.tieredstorage.commons.Chunk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class ChunkIndexBuilderCommonTest {
    protected static final int NON_EMPTY_FILE_SIZE = 250;
    protected static final int EMPTY_FILE_SIZE = 0;

    protected AbstractChunkIndexBuilder normalInitedChunkIndexBuilder;
    protected AbstractChunkIndexBuilder emptyFileChunkIndexBuilder;

    @BeforeEach
    protected abstract void init();

    @Test
    void addChunkToAlreadyFinishedIndex() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.finish(113);

        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.addChunk(113))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Cannot add chunk to already finished index");
    }

    @Test
    void addNonPositiveSizedChunk() {
        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.addChunk(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Transformed chunk size must be non-negative, -1 given");
    }

    @Test
    void addSupposedToBeFinalChunkAsNonFinal() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);

        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.addChunk(113))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This must be final chunk. Call `finish` instead.");
    }

    @Test
    void finishedAlreadyFinishedIndex() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.finish(113);

        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.finish(113))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Cannot finish already finished index");
    }

    @Test
    void finishWithNegativeSizedChunk() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);

        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.finish(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Transformed chunk size must be non-negative, -1 given");
    }

    @Test
    void finishWithNonFinalChunk() {
        normalInitedChunkIndexBuilder.addChunk(113);

        assertThatThrownBy(() -> normalInitedChunkIndexBuilder.finish(113))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This cannot be final chunk: not enough chunks to cover original file. "
                + "Call `addChunk` instead.");
    }

    @Test
    void findForNegativeOffset() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);
        final ChunkIndex index = normalInitedChunkIndexBuilder.finish(113);

        assertThatThrownBy(() -> index.findChunkForOriginalOffset(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Offset must be non-negative, -1 given");
    }

    @Test
    void findForEmptyFile() {
        final ChunkIndex index = emptyFileChunkIndexBuilder.finish(0);

        assertThat(index.findChunkForOriginalOffset(0)).isNull();
        assertThat(index.findChunkForOriginalOffset(1)).isNull();

        assertThat(index.chunks()).containsExactly(
            new Chunk(0, 0, 0, 0, 0)
        );
    }

    @Test
    void findBeyondFileBorder() {
        normalInitedChunkIndexBuilder.addChunk(113);
        normalInitedChunkIndexBuilder.addChunk(113);
        final ChunkIndex index = normalInitedChunkIndexBuilder.finish(113);

        assertThat(index.findChunkForOriginalOffset(NON_EMPTY_FILE_SIZE)).isNull();
        assertThat(index.findChunkForOriginalOffset(NON_EMPTY_FILE_SIZE + 1)).isNull();
    }
}
