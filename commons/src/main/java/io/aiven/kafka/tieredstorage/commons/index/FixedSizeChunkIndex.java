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

import java.util.List;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.commons.Chunk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The chunk index for transformed chunks of fixed size.
 *
 * <p>The most important quality is that both original and transformed chunks
 * are of fixed sizes, except maybe for the final ones,
 * which are specified and stored explicitly.
 * For example, this is a result of encryption.
 *
 * <p>An example:
 * <pre>
 * Original file:        |  Transformed file:
 * [0-100)   - size 100  |  [0-110)   - size 110
 * [100-200) - size 100  |  [110-220) - size 110
 * [200-250) - size 50   |  [220-300) - size 80
 * </pre>
 *
 * <p>Once constructed, the object remains immutable.
 */
public class FixedSizeChunkIndex extends AbstractChunkIndex {
    @JsonProperty("transformedChunkSize")
    final int transformedChunkSize;

    @JsonProperty("finalTransformedChunkSize")
    private int finalTransformedChunkSize() {
        return finalTransformedChunkSize;
    }

    // This only a materialization for convenience and performance,
    // it should not be persisted.
    private final List<Chunk> chunks;

    @JsonCreator
    public FixedSizeChunkIndex(@JsonProperty("originalChunkSize")
                               final int originalChunkSize,
                               @JsonProperty("originalFileSize")
                               final int originalFileSize,
                               @JsonProperty("transformedChunkSize")
                               final int transformedChunkSize,
                               @JsonProperty("finalTransformedChunkSize")
                               final int finalTransformedChunkSize) {
        super(originalChunkSize, originalFileSize, finalTransformedChunkSize,
            chunkCount(originalChunkSize, originalFileSize));

        checkSize(transformedChunkSize, "Transformed chunk size");
        this.transformedChunkSize = transformedChunkSize;

        chunks = materializeChunks();
    }

    private static int chunkCount(final int originalChunkSize, final int originalFileSize) {
        // ceil
        return originalFileSize % originalChunkSize == 0
            ? originalFileSize / originalChunkSize
            : originalFileSize / originalChunkSize + 1;
    }

    public List<Chunk> chunks() {
        return chunks;
    }

    @Override
    // Override with care, this method is indirectly called from a constructor.
    protected final int transformedChunkSize(final int chunkI) {
        final boolean isFinalChunk = chunkI == chunkCount - 1;
        return isFinalChunk ? finalTransformedChunkSize : transformedChunkSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FixedSizeChunkIndex that = (FixedSizeChunkIndex) o;
        return originalChunkSize == that.originalChunkSize
            && originalFileSize == that.originalFileSize
            && transformedChunkSize == that.transformedChunkSize
            && finalTransformedChunkSize == that.finalTransformedChunkSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalChunkSize, originalFileSize, transformedChunkSize, finalTransformedChunkSize);
    }
}
