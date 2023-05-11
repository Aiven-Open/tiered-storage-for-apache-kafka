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

package io.aiven.kafka.tieredstorage.commons;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.transform.OutboundTransformChain;
import io.aiven.kafka.tieredstorage.commons.transform.TransformFinisher;
import io.aiven.kafka.tieredstorage.commons.transform.TransformPipeline;

import org.apache.commons.io.input.BoundedInputStream;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class ChunkedLogSegmentManager {
    private final ObjectStorageFactory objectStorageFactory;
    private final ObjectKey objectKey;
    private final TransformPipeline pipeline;

    public ChunkedLogSegmentManager(final UniversalRemoteStorageManagerConfig config) {
        objectStorageFactory = config.objectStorageFactory();
        objectKey = new ObjectKey(config.keyPrefix());
        pipeline = TransformPipeline.newBuilder().fromConfig(config).build();
    }

    /**
     * Gets a chunk of a segment.
     *
     * @return an {@link InputStream} of the chunk, plain text (i.e. decrypted and decompressed).
     */
    public InputStream fetchChunk(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final SegmentManifest manifest,
                                  final int chunkId) throws IOException {
        final String segmentKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);
        final InputStream chunkContent = objectStorageFactory.fileFetcher().fetch(segmentKey, chunk.range());
        final OutboundTransformChain outboundTransformChain =
            pipeline.outboundTransformChain(chunkContent, manifest, chunk);
        return outboundTransformChain.complete().sequence();
    }

    public void uploadLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final LogSegmentData logSegmentData)
        throws RemoteStorageException {
        try {
            final TransformFinisher complete = pipeline.inboundTransformChain(logSegmentData.logSegment()).complete();
            try (final InputStream sis = complete.sequence()) {
                final String fileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
                objectStorageFactory.fileUploader().upload(sis, fileKey);
            }

            final SegmentManifest segmentManifest = pipeline.segmentManifest(complete);
            uploadManifest(remoteLogSegmentMetadata, segmentManifest);

            uploadIndexFile(remoteLogSegmentMetadata, Files.newInputStream(logSegmentData.offsetIndex()), OFFSET);
            uploadIndexFile(remoteLogSegmentMetadata, Files.newInputStream(logSegmentData.timeIndex()),
                TIMESTAMP);
            uploadIndexFile(remoteLogSegmentMetadata, Files.newInputStream(logSegmentData.producerSnapshotIndex()),
                PRODUCER_SNAPSHOT);
            if (logSegmentData.transactionIndex().isPresent()) {
                uploadIndexFile(remoteLogSegmentMetadata, Files.newInputStream(logSegmentData.transactionIndex().get()),
                    TRANSACTION);
            }
            uploadIndexFile(remoteLogSegmentMetadata, new ByteBufferInputStream(logSegmentData.leaderEpochIndex()),
                LEADER_EPOCH);
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final RemoteStorageManager.IndexType indexType) throws IOException {
        objectStorageFactory.fileUploader().upload(index,
            objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws IOException {
        final InputStream manifest = pipeline.serializeSegmentManifest(segmentManifest);
        final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        objectStorageFactory.fileUploader().upload(manifest, manifestFileKey);
    }

    public void deleteLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        try {
            for (final ObjectKey.Suffix suffix : ObjectKey.Suffix.values()) {
                objectStorageFactory.fileDeleter()
                    .delete(objectKey.key(remoteLogSegmentMetadata, suffix));
            }
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    public InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final int startPosition,
        final int endPosition
    ) throws RemoteStorageException {
        try {
            final InputStream manifest = objectStorageFactory.fileFetcher()
                .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST));

            final SegmentManifest segmentManifestV1 = pipeline.deserializeSegmentManifestContent(manifest);
            final FetchedChunksContent fetchedChunksContent = new FetchedChunksContent(
                this,
                remoteLogSegmentMetadata,
                segmentManifestV1,
                startPosition,
                endPosition);
            return new SequenceInputStream(fetchedChunksContent);
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final RemoteStorageManager.IndexType indexType) throws RemoteStorageException {
        try {
            return objectStorageFactory.fileFetcher()
                .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
        } catch (final IOException e) {
            // TODO: should be aligned with upstream implementation
            if (indexType == TRANSACTION) {
                return null;
            } else {
                throw new RemoteStorageException(e);
            }
        }
    }

    public static class FetchedChunksContent implements Enumeration<InputStream> {
        private final ChunkedLogSegmentManager chunkManager;
        private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;
        private final SegmentManifest manifest;
        private final int firstPosition;
        private final int lastPosition;
        final int startChunkId;
        final int lastChunkId;
        private final ChunkIndex chunkIndex;
        int currentChunkId;

        /**
         * @param chunkedLogSegmentManager provides chunk input to fetch from
         * @param remoteLogSegmentMetadata required by chunkManager
         * @param manifest                 provides to index to build response from
         * @param firstPosition            original offset range position to start from
         * @param lastPosition             original offset range position to finish
         */
        public FetchedChunksContent(final ChunkedLogSegmentManager chunkedLogSegmentManager,
                                    final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                    final SegmentManifest manifest,
                                    final int firstPosition,
                                    final int lastPosition) {
            this.chunkManager = Objects.requireNonNull(chunkedLogSegmentManager, "chunkManager cannot be null");
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
                return chunkManager.fetchChunk(remoteLogSegmentMetadata, manifest, chunkId);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
