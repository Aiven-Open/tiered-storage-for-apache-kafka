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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.transform.DetransformFinisher;
import io.aiven.kafka.tieredstorage.commons.transform.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.TransformFinisher;
import io.aiven.kafka.tieredstorage.commons.transform.TransformPipeline;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class UniversalRemoteStorageManager implements RemoteStorageManager, ChunkManager {
    private TransformPipeline pipeline;
    private ObjectStorageFactory objectStorageFactory;

    private ObjectKey objectKey;

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final UniversalRemoteStorageManagerConfig config = new UniversalRemoteStorageManagerConfig(configs);
        pipeline = TransformPipeline.newBuilder().fromConfig(config).build();
        objectStorageFactory = config.objectStorageFactory();
        objectKey = new ObjectKey(config.keyPrefix());
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata segmentMetadata,
                                   final LogSegmentData segmentData) throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(segmentData, "segmentData must not be null");
        try {
            final TransformFinisher complete = pipeline.inboundTransformChain(segmentData.logSegment()).complete();
            try (final var sis = complete.sequence()) {
                final String fileKey = objectKey.key(segmentMetadata, ObjectKey.Suffix.LOG);
                objectStorageFactory.fileUploader().upload(sis, fileKey);
            }

            final SegmentManifest segmentManifest = pipeline.segmentManifest(complete);
            uploadManifest(segmentMetadata, segmentManifest);
            uploadIndexFile(segmentMetadata,
                Files.newInputStream(segmentData.offsetIndex()), OFFSET);
            uploadIndexFile(segmentMetadata,
                Files.newInputStream(segmentData.timeIndex()), TIMESTAMP);
            uploadIndexFile(segmentMetadata,
                Files.newInputStream(segmentData.producerSnapshotIndex()), PRODUCER_SNAPSHOT);
            if (segmentData.transactionIndex().isPresent()) {
                uploadIndexFile(segmentMetadata,
                    Files.newInputStream(segmentData.transactionIndex().get()), TRANSACTION);
            }
            uploadIndexFile(segmentMetadata,
                new ByteBufferInputStream(segmentData.leaderEpochIndex()), LEADER_EPOCH);
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final IndexType indexType) throws IOException {
        objectStorageFactory.fileUploader().upload(index,
            objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws IOException {
        final InputStream manifestContent = pipeline.serializeSegmentManifest(segmentManifest);
        final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        objectStorageFactory.fileUploader().upload(manifestContent, manifestFileKey);
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        return this.fetchLogSegment(remoteLogSegmentMetadata, startPosition,
            remoteLogSegmentMetadata.segmentSizeInBytes());
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition,
                                       final int endPosition) throws RemoteStorageException {
        try {
            final InputStream manifest = objectStorageFactory.fileFetcher()
                .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST));

            final SegmentManifest segmentManifest = pipeline.deserializeSegmentManifestContent(manifest);
            final FetchChunkEnumeration fetchChunkEnumeration = new FetchChunkEnumeration(
                this,
                remoteLogSegmentMetadata,
                segmentManifest,
                startPosition,
                endPosition);
            return new SequenceInputStream(fetchChunkEnumeration);
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream getChunk(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest manifest,
                                final int chunkId) throws IOException {
        final Chunk chunk = manifest.chunkIndex().chunks().get(chunkId);
        final InputStream segmentFile = objectStorageFactory.fileFetcher()
            .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG),
                chunk.transformedPosition,
                chunk.transformedPosition + chunk.transformedSize);
        final DetransformFinisher complete = pipeline.outboundTransformChain(
            segmentFile,
            manifest,
            List.of(chunk)
        ).complete();
        return complete.nextElement();
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType));
            return objectStorageFactory.fileFetcher().fetch(key);
        } catch (final IOException e) {
            throw new RemoteStorageException(e);
        }

    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
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

    @Override
    public void close() {
    }
}
