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

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.apache.commons.io.input.BoundedInputStream;

public class FetchChunkManager {
    private final ChunkManager chunkManager;
    private final String objectKeyPath;
    private final SegmentManifest manifest;
    private final ChunkIndex chunkIndex;
    final BytesRange range;

    /**
     *
     * @param chunkManager provides chunk input to fetch from
     * @param objectKeyPath required by chunkManager
     * @param manifest provides to index to build response from
     * @param range original offset range start/end position
     */
    public FetchChunkManager(final ChunkManager chunkManager,
                             final String objectKeyPath,
                             final SegmentManifest manifest,
                             final BytesRange range) {
        this.chunkManager = Objects.requireNonNull(chunkManager, "chunkManager cannot be null");
        this.objectKeyPath = Objects.requireNonNull(objectKeyPath, "objectKeyPath cannot be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest cannot be null");
        this.range = Objects.requireNonNull(range, "range cannot be null");

        this.chunkIndex = manifest.chunkIndex();
    }

    private Chunk getFirstChunk(final int fromPosition) {
        final Chunk firstChunk = chunkIndex.findChunkForOriginalOffset(fromPosition);
        if (firstChunk == null) {
            throw new IllegalArgumentException("Invalid start position " + fromPosition
                + " in segment path " + objectKeyPath);
        }
        return firstChunk;
    }

    private Chunk getLastChunk(final int endPosition) {
        final Chunk chunkForOriginalOffset = chunkIndex.findChunkForOriginalOffset(endPosition);
        if (chunkForOriginalOffset == null) {
            final List<Chunk> chunks = chunkIndex.chunks();
            return chunks.get(chunks.size() - 1);
        } else {
            return chunkForOriginalOffset;
        }
    }

    InputStream fetch() {
        try {
            final Chunk firstChunk = getFirstChunk(range.from);
            final Chunk lastChunk = getLastChunk(range.to);

            final var content = new SequenceInputStream(
                chunkManager.chunksContent(objectKeyPath, manifest, firstChunk.id, lastChunk.id));

            final int toSkipBeginning = range.from - firstChunk.originalPosition;
            content.skip(toSkipBeginning);
            return new BoundedInputStream(content, range.size());
        } catch (final StorageBackendException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
