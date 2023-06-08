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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectDeleter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectUploader;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;
import io.aiven.kafka.tieredstorage.transform.TransformPipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class RemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(RemoteStorageManager.class);

    private final Time time;

    private final Metrics metrics;

    private final Executor executor = new ForkJoinPool();

    private ObjectFetcher fetcher;
    private ObjectUploader uploader;
    private ObjectDeleter deleter;
    private boolean compressionEnabled;
    private boolean compressionHeuristic;
    private ChunkManager chunkManager;
    private ObjectKey objectKey;
    private TransformPipeline transformPipeline;

    private SegmentManifestProvider segmentManifestProvider;

    public RemoteStorageManager() {
        this(Time.SYSTEM);
    }

    // for testing
    RemoteStorageManager(final Time time) {
        this.time = time;
        metrics = new Metrics(time);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(configs);
        fetcher = config.storage();
        uploader = config.storage();
        deleter = config.storage();
        objectKey = new ObjectKey(config.keyPrefix());

        compressionEnabled = config.compressionEnabled();
        compressionHeuristic = config.compressionHeuristicEnabled();

        transformPipeline = TransformPipeline.newBuilder().fromConfig(config).build();

        chunkManager = new ChunkManager(
            fetcher,
            objectKey,
            config.chunkCache(),
            transformPipeline
        );
        segmentManifestProvider = new SegmentManifestProvider(
            objectKey,
            config.segmentManifestCacheSize(),
            config.segmentManifestCacheRetention(),
            fetcher,
            transformPipeline.objectMapper(),
            executor);
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        metrics.recordSegmentCopy();

        final long startedMs = time.milliseconds();

        try {
            final var inboundTransformChain = transformPipeline.inboundTransformChain(logSegmentData.logSegment());
            final var transformFinisher = inboundTransformChain.complete();
            uploadSegmentLog(remoteLogSegmentMetadata, transformFinisher);

            final SegmentManifest segmentManifest = transformPipeline.segmentManifest(transformFinisher.chunkIndex());
            uploadManifest(remoteLogSegmentMetadata, segmentManifest);

            final InputStream offsetIndex = Files.newInputStream(logSegmentData.offsetIndex());
            uploadIndexFile(remoteLogSegmentMetadata, offsetIndex, OFFSET);
            final InputStream timeIndex = Files.newInputStream(logSegmentData.timeIndex());
            uploadIndexFile(remoteLogSegmentMetadata, timeIndex, TIMESTAMP);
            final InputStream producerSnapshotIndex = Files.newInputStream(logSegmentData.producerSnapshotIndex());
            uploadIndexFile(remoteLogSegmentMetadata, producerSnapshotIndex, PRODUCER_SNAPSHOT);
            if (logSegmentData.transactionIndex().isPresent()) {
                final InputStream transactionIndex = Files.newInputStream(logSegmentData.transactionIndex().get());
                uploadIndexFile(remoteLogSegmentMetadata, transactionIndex, TRANSACTION);
            }
            final ByteBufferInputStream leaderEpoch = new ByteBufferInputStream(logSegmentData.leaderEpochIndex());
            uploadIndexFile(remoteLogSegmentMetadata, leaderEpoch, LEADER_EPOCH);
        } catch (final StorageBackendException | IOException e) {
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentCopyTime(startedMs, time.milliseconds());
    }

    boolean requiresCompression(final LogSegmentData logSegmentData) {
        boolean requiresCompression = false;
        if (compressionEnabled) {
            if (compressionHeuristic) {
                try {
                    final File segmentFile = logSegmentData.logSegment().toFile();
                    final boolean alreadyCompressed = SegmentCompressionChecker.check(segmentFile);
                    requiresCompression = !alreadyCompressed;
                } catch (final InvalidRecordBatchException e) {
                    // Log and leave value as false to upload uncompressed.
                    log.warn("Failed to check compression on log segment: {}", logSegmentData.logSegment(), e);
                }
            } else {
                requiresCompression = true;
            }
        }
        return requiresCompression;
    }

    private void uploadSegmentLog(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final TransformFinisher transformFinisher)
        throws IOException, StorageBackendException {
        final String fileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
        try (final var sis = transformFinisher.toInputStream()) {
            uploader.upload(sis, fileKey);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final IndexType indexType)
        throws StorageBackendException, IOException {
        final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType));
        try (index) {
            uploader.upload(index, key);
        }
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws StorageBackendException, IOException {
        final byte[] manifestBytes = transformPipeline.objectMapper().writeValueAsBytes(segmentManifest);
        try (final var manifestContent = new ByteArrayInputStream(manifestBytes)) {
            final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);
            uploader.upload(manifestContent, manifestFileKey);
        }
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        return this.fetchLogSegment(
            remoteLogSegmentMetadata,
            startPosition,
            remoteLogSegmentMetadata.segmentSizeInBytes() - 1
        );
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition,
                                       final int endPosition) throws RemoteStorageException {
        metrics.recordSegmentFetch();

        try {
            final SegmentManifest segmentManifest = segmentManifestProvider.get(remoteLogSegmentMetadata);

            final BytesRange range = BytesRange.of(
                startPosition,
                Math.min(endPosition, remoteLogSegmentMetadata.segmentSizeInBytes() - 1)
            );
            return new FetchChunkEnumeration(chunkManager, remoteLogSegmentMetadata, segmentManifest, range)
                .toInputStream();
        } catch (final StorageBackendException | IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType));
            return fetcher.fetch(key);
        } catch (final StorageBackendException e) {
            // TODO: should be aligned with upstream implementation
            if (indexType == TRANSACTION) {
                return null;
            } else {
                throw new RemoteStorageException(e);
            }
        }

    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        try {
            for (final ObjectKey.Suffix suffix : ObjectKey.Suffix.values()) {
                final String key = objectKey.key(remoteLogSegmentMetadata, suffix);
                deleter.delete(key);
            }
        } catch (final StorageBackendException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public void close() {
        metrics.close();
    }
}
