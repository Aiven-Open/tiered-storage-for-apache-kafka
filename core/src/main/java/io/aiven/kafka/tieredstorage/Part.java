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
import java.util.Objects;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

public class Part {
    public final int firstChunkId;
    public final BytesRange range;
    public final List<Chunk> chunks;

    private final ChunkIndex chunkIndex;
    private final int partSize;
    private final int finalChunkId;

    public Part(final ChunkIndex chunkIndex,
                final Chunk chunk,
                final int partSize) {
        this.chunkIndex = chunkIndex;
        this.partSize = partSize;
        this.finalChunkId = chunkIndex.chunks().size() - 1;

        this.firstChunkId = Math.min((chunk.id / partSize) * partSize, finalChunkId);
        final var firstChunk = chunkIndex.chunks().get(firstChunkId);
        final var lastChunkId = Math.min(firstChunkId + partSize - 1, finalChunkId);
        final var lastChunk = chunkIndex.chunks().get(lastChunkId);
        this.range = BytesRange.of(firstChunk.range().from, lastChunk.range().to);
        this.chunks = chunkIndex.chunks().subList(firstChunkId, lastChunkId + 1);
    }

    private Part(final ChunkIndex chunkIndex,
                 final int partSize,
                 final int finalChunkId,
                 final int firstChunkId,
                 final BytesRange range,
                 final List<Chunk> chunks) {
        this.chunkIndex = chunkIndex;
        this.partSize = partSize;
        this.finalChunkId = finalChunkId;

        this.firstChunkId = firstChunkId;
        this.range = range;
        this.chunks = chunks;
    }

    public BytesRange range() {
        return range;
    }

    // optional?
    public Optional<Part> next() {
        final var currentLastChunkId = firstChunkId + partSize - 1;
        if (currentLastChunkId >= finalChunkId) {
            return Optional.empty();
        } else {
            final var nextFirstChunkId = Math.min(firstChunkId + partSize, finalChunkId);
            final var firstChunk = chunkIndex.chunks().get(nextFirstChunkId);
            final var nextLastChunkId = Math.min(nextFirstChunkId + partSize - 1, finalChunkId);
            final var lastChunk = chunkIndex.chunks().get(nextLastChunkId);
            final var range = BytesRange.of(firstChunk.range().from, lastChunk.range().to);
            final var chunks = chunkIndex.chunks().subList(nextFirstChunkId, nextLastChunkId + 1);
            final var part = new Part(chunkIndex, partSize, finalChunkId, nextFirstChunkId, range, chunks);
            return Optional.of(part);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Part part = (Part) o;
        return firstChunkId == part.firstChunkId
            && Objects.equals(range, part.range)
            && Objects.equals(chunks, part.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstChunkId, range, chunks);
    }

    public int startPosition() {
        return chunks.get(0).originalPosition;
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
