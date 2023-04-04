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

import java.util.List;
import java.util.Objects;

import io.aiven.kafka.tiered.storage.commons.chunkindex.serde.TransformedChunksDeserializer;
import io.aiven.kafka.tiered.storage.commons.chunkindex.serde.TransformedChunksSerializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * The chunk index for transformed chunks of variable size.
 *
 * <p>The most important quality is that while the original file chunks
 * are of the same size (apart from maybe the final one), the transformed chunks
 * have different sizes that must be specified and stored explicitly.
 * For example, this is a result of compression.
 *
 * <p>An example:
 * <pre>
 * Original file:        |  Transformed file:
 * [0-100)   - size 100  |  [0-30)  - size 30
 * [100-200) - size 100  |  [30-50) - size 20
 * [200-250) - size 50   |  [50-60) - size 10
 * </pre>
 *
 * <p>Once constructed, the object remains immutable.
 */
public class VariableSizeChunkIndex extends AbstractChunkIndex {
    @JsonProperty("transformedChunks")
    @JsonSerialize(using = TransformedChunksSerializer.class)
    private final List<Integer> transformedChunks;

    // This only a materialization for convenience and performance,
    // it should not be persisted.
    private final List<Chunk> chunks;

    // TODO consider storing and caching chunks encoded when they are fetched from remote storage
    // This will have smaller memory footprint than boxed integers.
    @JsonCreator
    public VariableSizeChunkIndex(@JsonProperty("originalChunkSize")
                                  final int originalChunkSize,
                                  @JsonProperty("originalFileSize")
                                  final int originalFileSize,
                                  @JsonProperty("transformedChunks")
                                  @JsonDeserialize(using = TransformedChunksDeserializer.class)
                                  final List<Integer> transformedChunks) {
        super(originalChunkSize, originalFileSize,
            finalTransformedChunkSize(Objects.requireNonNull(transformedChunks, "transformedChunks cannot be null")),
            transformedChunks.size());
        this.transformedChunks = transformedChunks;

        chunks = materializeChunks();
    }

    private static int finalTransformedChunkSize(final List<Integer> transformedChunks) {
        return transformedChunks.get(transformedChunks.size() - 1);
    }

    public List<Chunk> chunks() {
        return chunks;
    }

    @Override
    // Override with care, this method is indirectly called from a constructor.
    protected final int transformedChunkSize(final int chunkI) {
        return transformedChunks.get(chunkI);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final VariableSizeChunkIndex that = (VariableSizeChunkIndex) o;
        return originalChunkSize == that.originalChunkSize
            && originalFileSize == that.originalFileSize
            && Objects.equals(transformedChunks, that.transformedChunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalChunkSize, originalFileSize, transformedChunks);
    }
}
