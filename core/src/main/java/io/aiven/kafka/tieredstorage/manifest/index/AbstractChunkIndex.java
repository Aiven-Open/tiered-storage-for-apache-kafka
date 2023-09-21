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

package io.aiven.kafka.tieredstorage.manifest.index;

import java.util.ArrayList;
import java.util.List;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractChunkIndex implements ChunkIndex {
    @JsonProperty("originalChunkSize")
    protected final int originalChunkSize;
    @JsonProperty("originalFileSize")
    protected final int originalFileSize;
    protected final int finalTransformedChunkSize;
    protected final int chunkCount;

    protected AbstractChunkIndex(final int originalChunkSize,
                                 final int originalFileSize,
                                 final int finalTransformedChunkSize,
                                 final int chunkCount) {
        checkSizePositive(originalChunkSize, "Original chunk size");
        this.originalChunkSize = originalChunkSize;

        checkSizeNonNegative(originalFileSize, "Original file size");
        this.originalFileSize = originalFileSize;

        checkSizeNonNegative(finalTransformedChunkSize, "Final transformed chunk size");
        this.finalTransformedChunkSize = finalTransformedChunkSize;

        this.chunkCount = chunkCount;
    }

    // Override with care, this method is indirectly called from a constructor.
    protected final List<Chunk> materializeChunks() {
        final List<Chunk> chunks = new ArrayList<>(chunkCount);
        int originalPosition = 0;
        int transformedPosition = 0;
        if (chunkCount == 0) {
            chunks.add(new Chunk(0, 0, 0, 0, 0));
        } else {
            for (int chunkI = 0; chunkI < chunkCount; chunkI++) {
                final int originalSize = originalChunkSize(chunkI);
                final int transformedSize = transformedChunkSize(chunkI);
                chunks.add(new Chunk(
                    chunkI,
                    originalPosition, originalSize,
                    transformedPosition, transformedSize
                ));
                originalPosition += originalSize;
                transformedPosition += transformedSize;
            }
        }
        return List.copyOf(chunks);  // make unmodifiable
    }

    @Override
    public Chunk findChunkForOriginalOffset(final int offset) {
        checkOffset(offset);
        // This also covers "originalFileSize == 0".
        if (offset >= originalFileSize) {
            return null;
        }

        // Find the chunk index that corresponds to the offset in the original file
        // while counting the original and transformed chunk start position.
        int chunkI = 0;
        int curOriginalChunkPosition = 0;
        int curTransformedChunkPosition = 0;
        for (; chunkI < chunkCount; chunkI++) {
            // The situation we're looking for:
            //     chunkI--|      |--1st offset beyond current chunk
            //             v      v
            // |-----|-----|--*--|-----|-----
            //                ^
            //             offset
            final int firstOffsetBeyondCurOriginalChunk = (chunkI + 1) * originalChunkSize;
            if (offset < firstOffsetBeyondCurOriginalChunk) {
                break;
            }

            curOriginalChunkPosition += originalChunkSize(chunkI);
            curTransformedChunkPosition += transformedChunkSize(chunkI);
        }

        return new Chunk(
            chunkI,
            curOriginalChunkPosition,
            originalChunkSize(chunkI),
            curTransformedChunkPosition,
            transformedChunkSize(chunkI)
        );
    }

    @Override
    public List<Chunk> listChunksForRange(final BytesRange bytesRange) {
        Chunk current;
        final var result = new ArrayList<Chunk>();
        for (int i = bytesRange.from; i < bytesRange.to && i < originalFileSize; i += current.originalSize) {
            current = findChunkForOriginalOffset(i);
            result.add(current);
        }
        return result;
    }

    private int originalChunkSize(final int chunkI) {
        final boolean isFinalChunk = chunkI == chunkCount - 1;
        return isFinalChunk ? (originalFileSize - (chunkCount - 1) * originalChunkSize) : originalChunkSize;
    }

    protected abstract int transformedChunkSize(final int chunkI);

    protected static void checkSizeNonNegative(final int size, final String name) {
        if (size < 0) {
            throw new IllegalArgumentException(
                name + " must be non-negative, " + size + " given");
        }
    }

    protected static void checkSizePositive(final int size, final String name) {
        if (size <= 0) {
            throw new IllegalArgumentException(
                name + " must be positive, " + size + " given");
        }
    }

    protected final void checkOffset(final int offset) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                "Offset must be non-negative, " + offset + " given");
        }
    }
}
