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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyPair;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManagerFactory;
import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestProvider;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.DataKeyDeserializer;
import io.aiven.kafka.tieredstorage.manifest.serde.DataKeySerializer;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaKeyReader;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectDeleter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectUploader;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.BaseDetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DecryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.DetransformFinisher;
import io.aiven.kafka.tieredstorage.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class RemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(RemoteStorageManager.class);

    private final Time time;

    private Metrics metrics;

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
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(configs);
        final MetricConfig metricConfig = new MetricConfig()
            .samples(config.getInt(METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(METRICS_RECORDING_LEVEL_CONFIG)));
        metrics = new Metrics(time, metricConfig);
        setStorage(config.storage());
        objectKey = new ObjectKey(config.keyPrefix());
        encryptionEnabled = config.encryptionEnabled();
        if (encryptionEnabled) {
            final Map<String, KeyPair> keyRing = new HashMap<>();
            config.encryptionKeyRing().forEach((keyId, keyPaths) ->
                keyRing.put(keyId, RsaKeyReader.read(keyPaths.publicKey, keyPaths.privateKey)
            ));
            rsaEncryptionProvider = new RsaEncryptionProvider(config.encryptionKeyPairId(), keyRing);
            aesEncryptionProvider = new AesEncryptionProvider();
        }
        final ChunkManagerFactory chunkManagerFactory = new ChunkManagerFactory();
        chunkManagerFactory.configure(configs);
        chunkManager = chunkManagerFactory.initChunkManager(fetcher, aesEncryptionProvider);
        chunkSize = config.chunkSize();
        compressionEnabled = config.compressionEnabled();
        compressionHeuristic = config.compressionHeuristicEnabled();

        mapper = getObjectMapper();

        segmentManifestProvider = new SegmentManifestProvider(
            config.segmentManifestCacheSize(),
            config.segmentManifestCacheRetention(),
            fetcher,
            mapper,
            executor);
    }

    // for testing
    void setStorage(final StorageBackend storage) {
        fetcher = storage;
        uploader = storage;
        deleter = storage;
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        if (encryptionEnabled) {
            final SimpleModule simpleModule = new SimpleModule();
            simpleModule.addSerializer(SecretKey.class,
                new DataKeySerializer(rsaEncryptionProvider::encryptDataKey));
            simpleModule.addDeserializer(SecretKey.class,
                new DataKeyDeserializer(rsaEncryptionProvider::decryptDataKey));
            objectMapper.registerModule(simpleModule);
        }
        return objectMapper;
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        log.info("Copying log segment data, metadata: {}", remoteLogSegmentMetadata);

        metrics.recordSegmentCopy(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            remoteLogSegmentMetadata.segmentSizeInBytes());

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
            uploadIndexFile(remoteLogSegmentMetadata, offsetIndex, OFFSET, encryptionMetadata);
            final InputStream timeIndex = Files.newInputStream(logSegmentData.timeIndex());
            uploadIndexFile(remoteLogSegmentMetadata, timeIndex, TIMESTAMP, encryptionMetadata);
            final InputStream producerSnapshotIndex = Files.newInputStream(logSegmentData.producerSnapshotIndex());
            uploadIndexFile(remoteLogSegmentMetadata, producerSnapshotIndex, PRODUCER_SNAPSHOT, encryptionMetadata);
            if (logSegmentData.transactionIndex().isPresent()) {
                final InputStream transactionIndex = Files.newInputStream(logSegmentData.transactionIndex().get());
                uploadIndexFile(remoteLogSegmentMetadata, transactionIndex, TRANSACTION, encryptionMetadata);
            }
            final ByteBufferInputStream leaderEpoch = new ByteBufferInputStream(logSegmentData.leaderEpochIndex());
            uploadIndexFile(remoteLogSegmentMetadata, leaderEpoch, LEADER_EPOCH, encryptionMetadata);
        } catch (final Exception e) {
            metrics.recordSegmentCopyError(remoteLogSegmentMetadata.remoteLogSegmentId()
                .topicIdPartition().topicPartition());
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentCopyTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        log.info("Copying log segment data completed successfully, metadata: {}", remoteLogSegmentMetadata);
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
            final var bytes = uploader.upload(sis, fileKey);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                ObjectKey.Suffix.LOG,
                bytes
            );

            log.trace("Uploaded segment log for {}, size: {}", remoteLogSegmentMetadata, bytes);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final IndexType indexType,
                                 final SegmentEncryptionMetadata encryptionMetadata)
        throws StorageBackendException, IOException {
        TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(index);
        if (encryptionEnabled) {
            final var dataKeyAndAAD = new DataKeyAndAAD(encryptionMetadata.dataKey(), encryptionMetadata.aad());
            transformEnum = new EncryptionChunkEnumeration(
                transformEnum,
                () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD));
        }
        final TransformFinisher transformFinisher =
            new TransformFinisher(transformEnum);

        final var suffix = ObjectKey.Suffix.fromIndexType(indexType);
        final String key = objectKey.key(remoteLogSegmentMetadata, suffix);
        try (final var in = transformFinisher.toInputStream()) {
            final var bytes = uploader.upload(in, key);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                suffix,
                bytes
            );

            log.trace("Uploaded index file {} for {}, size: {}", indexType, remoteLogSegmentMetadata, bytes);
        }
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws StorageBackendException, IOException {
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final String manifestFileKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
            final var bytes = uploader.upload(manifestContent, manifestFileKey);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                ObjectKey.Suffix.MANIFEST,
                bytes
            );

            log.trace("Uploaded segment manifest for {}, size: {}", remoteLogSegmentMetadata, bytes);
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
        try {
            final BytesRange range = BytesRange.of(
                startPosition,
                Math.min(endPosition, remoteLogSegmentMetadata.segmentSizeInBytes() - 1)
            );

            log.debug("Fetching log segment {} with range: {}", remoteLogSegmentMetadata, range);

            metrics.recordSegmentFetch(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                range.size());

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final var segmentKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);

            return new FetchChunkEnumeration(chunkManager, segmentKey, segmentManifest, range)
                .toInputStream();
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private SegmentManifest fetchSegmentManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws StorageBackendException, IOException {
        final String manifestKey = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);
        return segmentManifestProvider.get(manifestKey);
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            log.debug("Fetching index {} for {}", indexType, remoteLogSegmentMetadata);

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final String key = objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType));
            final var in = fetcher.fetch(key);

            DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(in);
            final Optional<SegmentEncryptionMetadata> encryptionMetadata = segmentManifest.encryption();
            if (encryptionMetadata.isPresent()) {
                detransformEnum = new DecryptionChunkEnumeration(
                    detransformEnum,
                    encryptionMetadata.get().ivSize(),
                    encryptedChunk -> aesEncryptionProvider.decryptionCipher(encryptedChunk, encryptionMetadata.get())
                );
            }
            final DetransformFinisher detransformFinisher = new DetransformFinisher(detransformEnum);
            return detransformFinisher.toInputStream();
        } catch (final Exception e) {
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

        log.info("Deleting log segment data for {}", remoteLogSegmentMetadata);

        metrics.recordSegmentDelete(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            remoteLogSegmentMetadata.segmentSizeInBytes());

        final long startedMs = time.milliseconds();

        try {
            for (final ObjectKey.Suffix suffix : ObjectKey.Suffix.values()) {
                final String key = objectKey.key(remoteLogSegmentMetadata, suffix);
                deleter.delete(key);
            }
        } catch (final Exception e) {
            metrics.recordSegmentDeleteError(remoteLogSegmentMetadata.remoteLogSegmentId()
                .topicIdPartition().topicPartition());
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentDeleteTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        log.info("Deleting log segment data for completed successfully {}", remoteLogSegmentMetadata);
    }

    @Override
    public void close() {
        metrics.close();
    }
}
