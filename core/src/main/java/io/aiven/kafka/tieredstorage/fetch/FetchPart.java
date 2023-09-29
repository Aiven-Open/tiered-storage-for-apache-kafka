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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

/**
 * A Part represents a range of chunks to be processed together.
 */
public class FetchPart {
    public final int firstChunkId;
    public final BytesRange range;
    public final List<Chunk> chunks;

    private final ChunkIndex chunkIndex;
    final int lastChunkId;
    private final int partSize;
    final int finalChunkId;

    public FetchPart(final ChunkIndex chunkIndex,
                     final Chunk chunk,
                     final int partSize) {
        Objects.requireNonNull(chunkIndex, "Chunk index cannot be null");
        if (chunkIndex.chunks().isEmpty()) {
            throw new IllegalArgumentException("Chunk index must contain at least one chunk");
        }
        this.chunkIndex = chunkIndex;
        Objects.requireNonNull(chunk, "Chunk cannot be null");
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be higher than zero.");
        }
        this.partSize = partSize;

        // set first and last chunk included in part, and final chunk (last in segment)
        this.finalChunkId = chunkIndex.chunks().size() - 1;
        if (chunk.id > finalChunkId) {
            throw new IllegalArgumentException("Chunk does not belong to this segment, chunk id "
                + chunk.id + " is larger than final chunk id: " + finalChunkId);
        }

        this.firstChunkId = Math.floorDiv(chunk.id, partSize) * partSize;

        final var firstChunk = chunkIndex.chunks().get(firstChunkId);

        lastChunkId = Math.min(firstChunkId + partSize - 1, finalChunkId);
        final var lastChunk = chunkIndex.chunks().get(lastChunkId);

        // set part ranges and chunk
        this.range = BytesRange.of(firstChunk.range().from, lastChunk.range().to);
        this.chunks = chunkIndex.chunks().subList(firstChunkId, lastChunkId + 1);
    }

    /**
     * @return Maybe the next part from a segment. Empty if no more parts are available.
     */
    public Optional<FetchPart> next() {
        if (lastChunkId == finalChunkId) {
            return Optional.empty();
        } else {
            final var nextFirstChunkId = Math.min(firstChunkId + partSize, finalChunkId);
            final var nextFirstChunk = chunkIndex.chunks().get(nextFirstChunkId);
            return Optional.of(new FetchPart(chunkIndex, nextFirstChunk, partSize));
        }
    }

    public int startPosition() {
        return chunks.get(0).originalPosition;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FetchPart part = (FetchPart) o;
        return firstChunkId == part.firstChunkId
            && Objects.equals(range, part.range)
            && Objects.equals(chunks, part.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstChunkId, range, chunks);
    }

    @Override
    public String toString() {
        return "Part{"
            + "firstChunkId=" + firstChunkId
            + ", range=" + range
            + ", chunks=" + chunks.size()
            + '}';
    }
}
