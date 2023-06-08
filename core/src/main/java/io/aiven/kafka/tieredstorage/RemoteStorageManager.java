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

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
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

import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.DataKeyDeserializer;
import io.aiven.kafka.tieredstorage.manifest.serde.DataKeySerializer;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectDeleter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectUploader;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.FetchChunkInputStream;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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
    private boolean encryptionEnabled;
    private int chunkSize;
    private RsaEncryptionProvider rsaEncryptionProvider;
    private AesEncryptionProvider aesEncryptionProvider;
    private ObjectMapper mapper;
    private ChunkManager chunkManager;
    private ObjectKey objectKey;

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
        encryptionEnabled = config.encryptionEnabled();
        if (encryptionEnabled) {
            rsaEncryptionProvider = RsaEncryptionProvider.of(
                config.encryptionPublicKeyFile(),
                config.encryptionPrivateKeyFile()
            );
            aesEncryptionProvider = new AesEncryptionProvider();
        }
        chunkManager = new ChunkManager(
            fetcher,
            objectKey,
            aesEncryptionProvider,
            config.chunkCache()
        );

        chunkSize = config.chunkSize();
        compressionEnabled = config.compressionEnabled();
        compressionHeuristic = config.compressionHeuristicEnabled();

        mapper = getObjectMapper();

        segmentManifestProvider = new SegmentManifestProvider(
            objectKey,
            config.segmentManifestCacheSize(),
            config.segmentManifestCacheRetention(),
            fetcher,
            mapper,
            executor);
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        if (encryptionEnabled) {
            final SimpleModule simpleModule = new SimpleModule();
            simpleModule.addSerializer(SecretKey.class, new DataKeySerializer(rsaEncryptionProvider::encryptDataKey));
            simpleModule.addDeserializer(SecretKey.class, new DataKeyDeserializer(
                b -> new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(b), "AES")));
            objectMapper.registerModule(simpleModule);
        }
        return objectMapper;
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        metrics.recordSegmentCopy();

        final long startedMs = time.milliseconds();

        try {
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
                Files.newInputStream(logSegmentData.logSegment()), chunkSize);
            SegmentEncryptionMetadataV1 encryptionMetadata = null;
            final boolean requiresCompression = requiresCompression(logSegmentData);
            if (requiresCompression) {
                transformEnum = new CompressionChunkEnumeration(transformEnum);
            }
            if (encryptionEnabled) {
                final DataKeyAndAAD dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD));
                encryptionMetadata = new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad);
            }
            final TransformFinisher transformFinisher =
                new TransformFinisher(transformEnum, remoteLogSegmentMetadata.segmentSizeInBytes());
            uploadSegmentLog(remoteLogSegmentMetadata, transformFinisher);

            final ChunkIndex chunkIndex = transformFinisher.chunkIndex();
            final SegmentManifest segmentManifest =
                new SegmentManifestV1(chunkIndex, requiresCompression, encryptionMetadata);
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
        try (final var sis = new SequenceInputStream(transformFinisher)) {
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
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
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
            return new FetchChunkInputStream(chunkManager, remoteLogSegmentMetadata, segmentManifest, range);
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
