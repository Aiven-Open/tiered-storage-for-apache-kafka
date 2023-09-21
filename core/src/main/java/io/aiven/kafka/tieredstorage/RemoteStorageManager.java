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
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManagerFactory;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1Builder;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestProvider;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataBuilder;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaKeyReader;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectDeleter;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
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
import io.aiven.kafka.tieredstorage.transform.KeyNotFoundRuntimeException;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;

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
    private ObjectKeyFactory objectKeyFactory;
    private SegmentCustomMetadataSerde customMetadataSerde;
    private Set<SegmentCustomMetadataField> customMetadataFields;

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
        objectKeyFactory = new ObjectKeyFactory(config.keyPrefix(), config.keyPrefixMask());
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

        customMetadataSerde = new SegmentCustomMetadataSerde();
        customMetadataFields = config.customMetadataKeysIncluded();
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
        objectMapper.registerModule(KafkaTypeSerdeModule.create());
        if (encryptionEnabled) {
            objectMapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));
        }
        return objectMapper;
    }

    @Override
    public Optional<CustomMetadata> copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                       final LogSegmentData logSegmentData)
        throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        log.info("Copying log segment data, metadata: {}", remoteLogSegmentMetadata);

        final var customMetadataBuilder =
            new SegmentCustomMetadataBuilder(customMetadataFields, objectKeyFactory, remoteLogSegmentMetadata);

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
            uploadSegmentLog(remoteLogSegmentMetadata, transformFinisher, customMetadataBuilder);

            final ChunkIndex chunkIndex = transformFinisher.chunkIndex();
            final SegmentIndexesV1 segmentIndexes = uploadIndexes(
                remoteLogSegmentMetadata, logSegmentData, encryptionMetadata, customMetadataBuilder);
            final SegmentManifest segmentManifest = new SegmentManifestV1(
                chunkIndex, segmentIndexes, requiresCompression, encryptionMetadata, remoteLogSegmentMetadata);
            uploadManifest(remoteLogSegmentMetadata, segmentManifest, customMetadataBuilder);

        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }

        metrics.recordSegmentCopyTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        final var customMetadata = buildCustomMetadata(customMetadataBuilder);

        log.info("Copying log segment data completed successfully, metadata: {}", remoteLogSegmentMetadata);

        return customMetadata;
    }

    private SegmentIndexesV1 uploadIndexes(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData segmentData,
        final SegmentEncryptionMetadataV1 encryptionMeta,
        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) throws IOException, RemoteStorageException, StorageBackendException {
        final List<InputStream> indexes = new ArrayList<>(IndexType.values().length);
        final SegmentIndexesV1Builder segmentIndexBuilder = new SegmentIndexesV1Builder();
        final var offsetIndex = transformIndex(
            IndexType.OFFSET,
            Files.newInputStream(segmentData.offsetIndex()),
            indexSize(segmentData.offsetIndex()),
            encryptionMeta,
            segmentIndexBuilder
        );
        indexes.add(offsetIndex);
        final var timeIndex = transformIndex(
            IndexType.TIMESTAMP,
            Files.newInputStream(segmentData.timeIndex()),
            indexSize(segmentData.timeIndex()),
            encryptionMeta,
            segmentIndexBuilder
        );
        indexes.add(timeIndex);
        final var producerSnapshotIndex = transformIndex(
            IndexType.PRODUCER_SNAPSHOT,
            Files.newInputStream(segmentData.producerSnapshotIndex()),
            indexSize(segmentData.producerSnapshotIndex()),
            encryptionMeta,
            segmentIndexBuilder
        );
        indexes.add(producerSnapshotIndex);
        final var leaderEpoch = transformIndex(
            IndexType.LEADER_EPOCH,
            new ByteBufferInputStream(segmentData.leaderEpochIndex()),
            segmentData.leaderEpochIndex().remaining(),
            encryptionMeta,
            segmentIndexBuilder
        );
        indexes.add(leaderEpoch);
        if (segmentData.transactionIndex().isPresent()) {
            final var transactionIndex = transformIndex(
                IndexType.TRANSACTION,
                Files.newInputStream(segmentData.transactionIndex().get()),
                indexSize(segmentData.transactionIndex().get()),
                encryptionMeta,
                segmentIndexBuilder
            );
            indexes.add(transactionIndex);
        }
        final var suffix = ObjectKeyFactory.Suffix.INDEXES;
        final ObjectKey key = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
        try (final var in = new SequenceInputStream(Collections.enumeration(indexes))) {
            final var bytes = uploader.upload(in, key);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                suffix,
                bytes
            );
            customMetadataBuilder.addUploadResult(suffix, bytes);

            log.debug("Uploaded indexes file for {}, size: {}", remoteLogSegmentMetadata, bytes);
        }
        return segmentIndexBuilder.build();
    }

    static int indexSize(final Path indexPath) throws RemoteStorageException {
        try {
            final var size = Files.size(indexPath);
            if (size > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                    "Index at path "
                        + indexPath
                        + " has size larger than Integer.MAX_VALUE");
            }
            return (int) size;
        } catch (final IOException e) {
            throw new RemoteStorageException("Error while getting index path size", e);
        }
    }

    private Optional<CustomMetadata> buildCustomMetadata(final SegmentCustomMetadataBuilder customMetadataBuilder) {
        final var customFields = customMetadataBuilder.build();
        if (!customFields.isEmpty()) {
            final var customMetadataBytes = customMetadataSerde.serialize(customFields);
            return Optional.of(new CustomMetadata(customMetadataBytes));
        } else {
            return Optional.empty();
        }
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
                                  final TransformFinisher transformFinisher,
                                  final SegmentCustomMetadataBuilder customMetadataBuilder)
        throws IOException, StorageBackendException {
        final ObjectKey fileKey = objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.LOG);
        try (final var sis = transformFinisher.toInputStream()) {
            final var bytes = uploader.upload(sis, fileKey);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                ObjectKeyFactory.Suffix.LOG,
                bytes
            );
            customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.LOG, bytes);

            log.debug("Uploaded segment log for {}, size: {}", remoteLogSegmentMetadata, bytes);
        }
    }

    InputStream transformIndex(final IndexType indexType,
                               final InputStream index,
                               final int size,
                               final SegmentEncryptionMetadata encryptionMetadata,
                               final SegmentIndexesV1Builder segmentIndexBuilder) {
        log.debug("Transforming index {} with size {}", indexType, size);
        if (size == 0) {
            return InputStream.nullInputStream();
        }
        TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(index, size);
        if (encryptionEnabled) {
            final var dataKeyAndAAD = new DataKeyAndAAD(encryptionMetadata.dataKey(), encryptionMetadata.aad());
            transformEnum = new EncryptionChunkEnumeration(
                transformEnum,
                () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD));
        }
        final var transformFinisher = new TransformFinisher(transformEnum, size);
        final var inputStream = transformFinisher.nextElement();
        segmentIndexBuilder.add(indexType, singleChunk(transformFinisher.chunkIndex()).range().size());
        return inputStream;
    }

    private Chunk singleChunk(final ChunkIndex chunkIndex) {
        final var chunks = chunkIndex.chunks();
        if (chunks.size() != 1) {
            throw new IllegalStateException("Single chunk expected when transforming indexes");
        }
        return chunks.get(0);
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest,
                                final SegmentCustomMetadataBuilder customMetadataBuilder)
        throws StorageBackendException, IOException {
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final ObjectKey manifestObjectKey =
            objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
            final var bytes = uploader.upload(manifestContent, manifestObjectKey);
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                ObjectKeyFactory.Suffix.MANIFEST,
                bytes
            );
            customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, bytes);

            log.debug("Uploaded segment manifest for {}, size: {}", remoteLogSegmentMetadata, bytes);
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

            log.trace("Fetching log segment {} with range: {}", remoteLogSegmentMetadata, range);

            metrics.recordSegmentFetch(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                range.size());

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final var suffix = ObjectKeyFactory.Suffix.LOG;
            final var segmentKey = objectKey(remoteLogSegmentMetadata, suffix);
            if (chunkManager instanceof ChunkCache) {
                ((ChunkCache<?>) chunkManager).startPrefetching(segmentKey, segmentManifest, range.from);
            }
            return new FetchChunkEnumeration(chunkManager, segmentKey, segmentManifest, range)
                .toInputStream();
        } catch (final KeyNotFoundException | KeyNotFoundRuntimeException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            log.trace("Fetching index {} for {}", indexType, remoteLogSegmentMetadata);

            final var segmentManifest = fetchSegmentManifest(remoteLogSegmentMetadata);

            final var key = objectKey(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.INDEXES);
            final var segmentIndex = segmentManifest.segmentIndexes().segmentIndex(indexType);
            if (segmentIndex == null) {
                throw new RemoteResourceNotFoundException("Index " + indexType + " not found on " + key);
            }
            final var in = fetcher.fetch(key, segmentIndex.range());

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
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final KeyNotFoundException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private ObjectKey objectKey(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final ObjectKeyFactory.Suffix suffix) {
        final ObjectKey segmentKey;
        if (remoteLogSegmentMetadata.customMetadata().isPresent()) {
            final var customMetadataBytes = remoteLogSegmentMetadata.customMetadata().get();
            final var fields = customMetadataSerde.deserialize(customMetadataBytes.value());
            segmentKey = objectKeyFactory.key(fields, remoteLogSegmentMetadata, suffix);
        } else {
            segmentKey = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
        }
        return segmentKey;
    }

    private SegmentManifest fetchSegmentManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws StorageBackendException, IOException {
        final ObjectKey manifestKey = objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);
        return segmentManifestProvider.get(manifestKey);
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {

        log.info("Deleting log segment data for {}", remoteLogSegmentMetadata);

        metrics.recordSegmentDelete(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            remoteLogSegmentMetadata.segmentSizeInBytes());

        final long startedMs = time.milliseconds();

        try {
            for (final ObjectKeyFactory.Suffix suffix : ObjectKeyFactory.Suffix.values()) {
                final ObjectKey key = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
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
