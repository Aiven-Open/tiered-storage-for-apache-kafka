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

import java.util.List;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An index that maps chunks in original and transformed files.
 *
 * <p>A transformed file is a file that has been through
 * some transformations like encryption and compression, chunk by chunk.
 *
 * <p>The original file is supposed to be split into chunks
 * of constant size (apart from the final one).
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = FixedSizeChunkIndex.class, name = "fixed"),
    @JsonSubTypes.Type(value = VariableSizeChunkIndex.class, name = "variable"),
})
public interface ChunkIndex {
    /**
     * For a given offset in the original file, finds the corresponding chunk.
     */
    Chunk findChunkForOriginalOffset(int offset);

    /**
     * Returns all chunks in the index.
     */
    List<Chunk> chunks();

    List<Chunk> listChunksForRange(BytesRange bytesRange);
}
