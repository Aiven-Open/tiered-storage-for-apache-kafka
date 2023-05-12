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

package io.aiven.kafka.tieredstorage.commons.transform;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.Chunk;
import io.aiven.kafka.tieredstorage.commons.ChunkManager;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;

import org.apache.commons.io.input.BoundedInputStream;

public class FetchChunkEnumeration implements Enumeration<InputStream> {
    private final ChunkManager chunkManager;
    private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;
    private final SegmentManifest manifest;
    private final int firstPosition;
    private final int lastPosition;
    final int startChunkId;
    final int lastChunkId;
    private final ChunkIndex chunkIndex;
    int currentChunkId;

    /**
     *
     * @param chunkManager provides chunk input to fetch from
     * @param remoteLogSegmentMetadata required by chunkManager
     * @param manifest provides to index to build response from
     * @param firstPosition original offset range position to start from
     * @param lastPosition original offset range position to finish
     */
    public FetchChunkEnumeration(final ChunkManager chunkManager,
                                 final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final SegmentManifest manifest,
                                 final int firstPosition,
                                 final int lastPosition) {
        this.chunkManager = Objects.requireNonNull(chunkManager, "chunkManager cannot be null");
        this.remoteLogSegmentMetadata =
            Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata cannot be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest cannot be null");
        this.firstPosition = firstPosition;
        this.lastPosition = lastPosition;

        this.chunkIndex = manifest.chunkIndex();

        final Chunk firstChunk = getFirstChunk(firstPosition);
        startChunkId = firstChunk.id;
        currentChunkId = startChunkId;
        final Chunk lastChunk = getLastChunk(lastPosition);
        lastChunkId = lastChunk.id;
    }

    private Chunk getFirstChunk(final int fromPosition) {
        final Chunk firstChunk = chunkIndex.findChunkForOriginalOffset(fromPosition);
        if (firstChunk == null) {
            throw new IllegalArgumentException("Invalid start position "
                + fromPosition + " in segment " + remoteLogSegmentMetadata);
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

    @Override
    public boolean hasMoreElements() {
        return currentChunkId <= lastChunkId;
    }

    @Override
    public InputStream nextElement() {
        if (!hasMoreElements()) {
            throw new NoSuchElementException();
        }

        InputStream chunkContent = getChunkContent(currentChunkId);

        final Chunk currentChunk = chunkIndex.chunks().get(currentChunkId);
        final int chunkStartPosition = currentChunk.originalPosition;
        final boolean isAtFirstChunk = currentChunkId == startChunkId;
        final boolean isAtLastChunk = currentChunkId == lastChunkId;
        final boolean isSingleChunk = isAtFirstChunk && isAtLastChunk;
        if (isSingleChunk) {
            final int toSkip = firstPosition - chunkStartPosition;
            try {
                chunkContent.skip(toSkip);
                final int chunkSize = lastPosition - firstPosition;
                chunkContent = new BoundedInputStream(chunkContent, chunkSize);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (isAtFirstChunk) {
                final int toSkip = firstPosition - chunkStartPosition;
                try {
                    chunkContent.skip(toSkip);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (isAtLastChunk) {
                final int chunkSize = lastPosition - chunkStartPosition;
                chunkContent = new BoundedInputStream(chunkContent, chunkSize);
            }
        }

        currentChunkId += 1;
        return chunkContent;
    }

    private InputStream getChunkContent(final int chunkId) {
        try {
            return chunkManager.getChunk(remoteLogSegmentMetadata, manifest, chunkId);
        } catch (final StorageBackEndException e) {
            throw new RuntimeException(e);
        }
    }
}
