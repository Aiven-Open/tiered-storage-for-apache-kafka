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

package io.aiven.kafka.tieredstorage.transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.kafka.common.utils.ByteBufferInputStream;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

public class FetchChunkEnumeration implements Enumeration<InputStream> {
    private final ChunkManager chunkManager;
    private final ObjectKey objectKey;
    private final SegmentManifest manifest;
    private final BytesRange range;
    final int startChunkId;
    final int lastChunkId;
    private final ChunkIndex chunkIndex;
    int currentChunkId;
    public boolean closed;

    /**
     *
     * @param chunkManager provides chunk input to fetch from
     * @param objectKey required by chunkManager
     * @param manifest provides to index to build response from
     * @param range original offset range start/end position
     */
    public FetchChunkEnumeration(final ChunkManager chunkManager,
                                 final ObjectKey objectKey,
                                 final SegmentManifest manifest,
                                 final BytesRange range) {
        this.chunkManager = Objects.requireNonNull(chunkManager, "chunkManager cannot be null");
        this.objectKey = Objects.requireNonNull(objectKey, "objectKey cannot be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest cannot be null");
        this.range = Objects.requireNonNull(range, "range cannot be null");

        this.chunkIndex = manifest.chunkIndex();

        final Chunk firstChunk = getFirstChunk(range.from);
        startChunkId = firstChunk.id;
        currentChunkId = startChunkId;
        final Chunk lastChunk = getLastChunk(range.to);
        lastChunkId = lastChunk.id;
    }

    private Chunk getFirstChunk(final int fromPosition) {
        final Chunk firstChunk = chunkIndex.findChunkForOriginalOffset(fromPosition);
        if (firstChunk == null) {
            throw new IllegalArgumentException("Invalid start position " + fromPosition
                + " in segment path " + objectKey);
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
        return !closed && currentChunkId <= lastChunkId;
    }

    @Override
    public InputStream nextElement() {
        if (!hasMoreElements()) {
            throw new NoSuchElementException();
        }

        final Chunk currentChunk = chunkIndex.chunks().get(currentChunkId);
        final int chunkStartPosition = currentChunk.originalPosition;
        final boolean isAtFirstChunk = currentChunkId == startChunkId;
        final boolean isAtLastChunk = currentChunkId == lastChunkId;
        final boolean isSingleChunk = isAtFirstChunk && isAtLastChunk;

        try {
            if (isSingleChunk) {
                final int toSkip = range.from - chunkStartPosition;
                final var range1 = BytesRange.of(toSkip, toSkip + range.size() - 1);
                return new ByteBufferInputStream(getChunkContent(currentChunkId, range1));
            } else {
                if (isAtFirstChunk) {
                    final int toSkip = range.from - chunkStartPosition;
                    final var range1 = BytesRange.of(toSkip, currentChunk.originalSize - 1);
                    return new ByteBufferInputStream(getChunkContent(currentChunkId, range1));
                }
                if (isAtLastChunk) {
                    final int chunkSize = range.to - chunkStartPosition + 1;
                    final var range1 = BytesRange.of(0, chunkSize - 1);
                    return new ByteBufferInputStream(getChunkContent(currentChunkId, range1));
                }
                final var range1 = BytesRange.of(0, currentChunk.originalSize - 1);
                return new ByteBufferInputStream(getChunkContent(currentChunkId, range1));
            }
        } finally {
            currentChunkId += 1;
        }
    }

    private ByteBuffer getChunkContent(final int chunkId, final BytesRange range) {
        try {
            final var chunk = chunkManager.getChunk(objectKey, manifest, chunkId, range);
            return chunk;
        } catch (final KeyNotFoundException e) {
            throw new KeyNotFoundRuntimeException(e);
        } catch (final StorageBackendException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream toInputStream() {
        return new LazySequenceInputStream(this);
    }

    public void close() {
        closed = true;
    }

    /**
     * This class overrides the behavior of {@link SequenceInputStream#close()} to avoid unnecessary calls to
     * {@link FetchChunkEnumeration#nextElement()} since {@link FetchChunkEnumeration} is supposed
     * to be lazy and does not create inout streams unless there was such a call.
     */
    private static class LazySequenceInputStream extends SequenceInputStream {
        private final FetchChunkEnumeration closeableEnumeration;

        LazySequenceInputStream(final FetchChunkEnumeration e) {
            super(e);
            this.closeableEnumeration = e;
        }

        public void close() throws IOException {
            closeableEnumeration.close();
            super.close();
        }
    }
}
