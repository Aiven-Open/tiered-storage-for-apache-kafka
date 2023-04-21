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

import org.apache.commons.io.input.BoundedInputStream;

public class FetchChunkEnumeration implements Enumeration<InputStream> {
    private final ChunkManager chunkManager;
    private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;
    private final SegmentManifest manifest;
    private final int toPosition;
    private final int fromPosition;
    private final int startChunkId;
    private final int endChunkId;
    private int currentChunkId;

    public FetchChunkEnumeration(final ChunkManager chunkManager,
                                 final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final SegmentManifest manifest,
                                 final int fromPosition,
                                 final int toPosition) {
        this.chunkManager = Objects.requireNonNull(chunkManager, "chunkManager cannot be null");
        this.remoteLogSegmentMetadata = Objects.requireNonNull(remoteLogSegmentMetadata,
            "remoteLogSegmentMetadata cannot be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest cannot be null");

        final ChunkIndex chunkIndex = manifest.chunkIndex();
        final Chunk startChunk = chunkIndex.findChunkForOriginalOffset(fromPosition);
        if (startChunk == null) {
            throw new IllegalArgumentException("Invalid start position "
                + fromPosition + " in segment " + remoteLogSegmentMetadata);
        }
        this.fromPosition = fromPosition;
        this.toPosition = toPosition;

        startChunkId = startChunk.id;
        currentChunkId = startChunkId;
        endChunkId = getLastChunk(toPosition, chunkIndex).id;
    }

    private Chunk getLastChunk(final int endPosition, final ChunkIndex chunkIndex) {
        final List<Chunk> chunks = chunkIndex.chunks();
        final Chunk chunkForOriginalOffset = chunkIndex.findChunkForOriginalOffset(endPosition);
        if (chunkForOriginalOffset == null) {
            return chunks.get(chunks.size() - 1);
        } else {
            return chunkForOriginalOffset;
        }
    }


    @Override
    public boolean hasMoreElements() {
        return currentChunkId <= endChunkId;
    }

    @Override
    public InputStream nextElement() {
        if (!hasMoreElements()) {
            throw new NoSuchElementException();
        }

        InputStream chunkContent = getChunkContent(currentChunkId);

        final Chunk currentChunk = manifest.chunkIndex().chunks().get(currentChunkId);
        final int chunkStartPosition = currentChunk.originalPosition;
        if (currentChunkId == startChunkId && currentChunkId == endChunkId) {
            final int toSkip = fromPosition - chunkStartPosition;
            try {
                chunkContent.skip(toSkip);
                chunkContent = new BoundedInputStream(chunkContent,
                    toPosition - chunkStartPosition - toSkip);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (currentChunkId == startChunkId) {
                final int toSkip = fromPosition - chunkStartPosition;
                try {
                    chunkContent.skip(toSkip);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (currentChunkId == endChunkId) {
                chunkContent = new BoundedInputStream(chunkContent,
                    toPosition - chunkStartPosition);
            }
        }
        currentChunkId += 1;
        return chunkContent;
    }

    private InputStream getChunkContent(final int chunkId) {
        try {
            return chunkManager.getChunk(remoteLogSegmentMetadata, manifest, chunkId);
        } catch (final IOException e) {
            //TODO handle properly
            throw new RuntimeException(e);
        }
    }
}
