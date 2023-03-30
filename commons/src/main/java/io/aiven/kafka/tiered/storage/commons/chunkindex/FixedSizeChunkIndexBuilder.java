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

public class FixedSizeChunkIndexBuilder extends AbstractChunkIndexBuilder {
    private final int transformedChunkSize;

    public FixedSizeChunkIndexBuilder(final int originalChunkSize,
                                      final int originalFileSize,
                                      final int transformedChunkSize) {
        super(originalChunkSize, originalFileSize);

        checkSize(transformedChunkSize, "Transformed chunk size");
        this.transformedChunkSize = transformedChunkSize;
    }

    @Override
    protected void addChunk0(final int transformedChunkSize) {
        // Sanity check.
        if (transformedChunkSize != this.transformedChunkSize) {
            throw new IllegalArgumentException("Non-final chunk must be of size " + this.transformedChunkSize
                + ", but " + transformedChunkSize + " given");
        }
    }

    @Override
    protected ChunkIndex finish0(final int finalTransformedChunkSize) {
        return new FixedSizeChunkIndex(
            originalChunkSize, originalFileSize, transformedChunkSize, finalTransformedChunkSize);
    }
}
