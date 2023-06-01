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

public abstract class AbstractChunkIndexBuilder {
    protected final int originalChunkSize;
    protected final int originalFileSize;

    private int chunksAdded = 0;
    private boolean finished = false;

    protected AbstractChunkIndexBuilder(final int originalChunkSize, final int originalFileSize) {
        checkSize(originalChunkSize, "Original chunk size");
        this.originalChunkSize = originalChunkSize;

        checkSize(originalFileSize, "Original file size");
        this.originalFileSize = originalFileSize;
    }

    /**
     * Add a non-final chunk to the index.
     * @param transformedChunkSize the size of the corresponding transformed chunk.
     */
    public final void addChunk(final int transformedChunkSize) {
        if (finished) {
            throw new IllegalStateException("Cannot add chunk to already finished index");
        }

        checkSize(transformedChunkSize, "Transformed chunk size");

        // Check that we expect only the final chunk at this point.
        if (remainOfOriginalFileSize() <= originalChunkSize) {
            throw new IllegalStateException("This must be final chunk. Call `finish` instead.");
        }

        addChunk0(transformedChunkSize);

        this.chunksAdded += 1;
    }

    protected abstract void addChunk0(int transformedChunkSize);

    /**
     * Add the final chunk to the index.
     * @param finalTransformedChunkSize the size of the corresponding transformed chunk.
     */
    public final ChunkIndex finish(final int finalTransformedChunkSize) {
        if (finished) {
            throw new IllegalStateException("Cannot finish already finished index");
        }

        checkSize(finalTransformedChunkSize, "Transformed chunk size");

        // Check that we expect only the final chunk at this point.
        if (remainOfOriginalFileSize() > originalChunkSize) {
            throw new IllegalStateException(
                "This cannot be final chunk: not enough chunks to cover original file. "
                    + "Call `addChunk` instead.");
        }

        final ChunkIndex result = finish0(finalTransformedChunkSize);

        this.chunksAdded += 1;  // technically not needed, but for consistency
        this.finished = true;

        return result;
    }

    protected abstract ChunkIndex finish0(final int finalTransformedChunkSize);

    protected final void checkSize(final int size, final String name) {
        if (size < 0) {
            throw new IllegalArgumentException(
                name + " must be non-negative, " + size + " given");
        }
    }

    protected final int remainOfOriginalFileSize() {
        return this.originalFileSize - (this.chunksAdded * this.originalChunkSize);
    }
}
