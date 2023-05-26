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

package io.aiven.kafka.tieredstorage.core.manifest.index;

import java.util.ArrayList;

public class VariableSizeChunkIndexBuilder extends AbstractChunkIndexBuilder {
    private final ArrayList<Integer> transformedChunks = new ArrayList<>();

    public VariableSizeChunkIndexBuilder(final int originalChunkSize,
                                         final int originalFileSize) {
        super(originalChunkSize, originalFileSize);
    }

    @Override
    protected void addChunk0(final int transformedChunkSize) {
        transformedChunks.add(transformedChunkSize);
    }

    @Override
    protected ChunkIndex finish0(final int finalTransformedChunkSize) {
        transformedChunks.add(finalTransformedChunkSize);
        return new VariableSizeChunkIndex(
            this.originalChunkSize,
            this.originalFileSize,
            this.transformedChunks
        );
    }

}
