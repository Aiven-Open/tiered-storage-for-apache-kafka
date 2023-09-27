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
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.Part;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchChunkEnumeration implements Enumeration<InputStream> {
    static final Logger log = LoggerFactory.getLogger(FetchChunkEnumeration.class);

    private final ChunkManager chunkManager;
    private final String objectKeyPath;
    private final SegmentManifest manifest;
    private final BytesRange range;
    final Part firstPart;
    final Chunk firstChunk;
    final Part lastPart;
    final Chunk lastChunk;
    private final ChunkIndex chunkIndex;
    Optional<Part> currentPart;
    public boolean closed;

    final int partSize;

    /**
     * @param chunkManager  provides chunk input to fetch from
     * @param objectKeyPath required by chunkManager
     * @param manifest      provides to index to build response from
     * @param range         original offset range start/end position
     * @param partSize      fetch part size
     */
    public FetchChunkEnumeration(final ChunkManager chunkManager,
                                 final String objectKeyPath,
                                 final SegmentManifest manifest,
                                 final BytesRange range,
                                 final int partSize) {
        this.chunkManager = Objects.requireNonNull(chunkManager, "chunkManager cannot be null");
        this.objectKeyPath = Objects.requireNonNull(objectKeyPath, "objectKeyPath cannot be null");
        this.manifest = Objects.requireNonNull(manifest, "manifest cannot be null");
        this.range = Objects.requireNonNull(range, "range cannot be null");
        this.partSize = partSize;

        this.chunkIndex = manifest.chunkIndex();

        firstChunk = getFirstChunk(range.from);
        firstPart = new Part(chunkIndex, firstChunk, this.partSize);
        currentPart = Optional.of(firstPart);
        lastChunk = getLastChunk(range.to);
        lastPart = new Part(chunkIndex, lastChunk, this.partSize);
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

    @Override
    public boolean hasMoreElements() {
        return !closed && currentPart.isPresent();
    }

    @Override
    public InputStream nextElement() {
        if (currentPart.isEmpty()) {
            throw new NoSuchElementException();
        }

        final InputStream partContent = partContent(currentPart.get());

        final int chunkStartPosition = currentPart.get().startPosition();
        final boolean isAtFirstPart = currentPart.get().equals(firstPart);
        final boolean isAtLastPart = currentPart.get().equals(lastPart);
        final boolean isSinglePart = isAtFirstPart && isAtLastPart;
        if (isSinglePart) {
            final int toSkip = range.from - chunkStartPosition;
            try {
                partContent.skip(toSkip);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }

            final int chunkSize = range.size();

            log.trace("Returning part {} with size {}", currentPart.get(), chunkSize);

            currentPart = Optional.empty();
            return new BoundedInputStream(partContent, chunkSize);
        } else {
            if (isAtFirstPart) {
                final int toSkip = range.from - chunkStartPosition;
                try {
                    partContent.skip(toSkip);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (isAtLastPart) {
                final int chunkSize = range.to - chunkStartPosition + 1;

                log.trace("Returning part {} with size {}", currentPart.get(), chunkSize);

                currentPart = Optional.empty();
                return new BoundedInputStream(partContent, chunkSize);
            }
        }

        log.trace("Returning part {} with size {}", currentPart.get(), currentPart.get().range.size());

        currentPart = currentPart.get().next();
        return partContent;
    }

    private InputStream partContent(final Part part) {
        try {
            return chunkManager.chunksContent(objectKeyPath, manifest, part);
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
