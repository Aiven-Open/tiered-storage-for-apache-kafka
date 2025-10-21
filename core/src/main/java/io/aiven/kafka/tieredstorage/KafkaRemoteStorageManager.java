/*
 * Copyright 2025 Aiven Oy
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
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.ChunkManagerFactory;
import io.aiven.kafka.tieredstorage.fetch.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.fetch.KeyNotFoundRuntimeException;
import io.aiven.kafka.tieredstorage.fetch.index.MemorySegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.index.SegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.SegmentManifestCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;
import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndex;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1Builder;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataBuilder;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
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
import io.aiven.kafka.tieredstorage.transform.RateLimitedInputStream;
import io.aiven.kafka.tieredstorage.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.transform.TransformFinisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bucket4j.Bucket;
import org.slf4j.Logger;

class KafkaRemoteStorageManager extends InternalRemoteStorageManager {
    private final StorageBackend storage;
    private final ObjectFetcher fetcher;
    private final ObjectUploader uploader;
    private final ObjectDeleter deleter;

    private final ObjectKeyFactory objectKeyFactory;
    private final SegmentCustomMetadataSerde customMetadataSerde;
    private final Set<SegmentCustomMetadataField> customMetadataFields;
    private final SegmentIndexesCache segmentIndexesCache;

    private ChunkManager chunkManager;
    private final int chunkSize;
    private final boolean compressionEnabled;
    private final boolean compressionHeuristic;
    private final boolean encryptionEnabled;
    private final AesEncryptionProvider aesEncryptionProvider;
    private final ObjectMapper mapper;

    private SegmentManifestCache segmentManifestCache;

    private final Bucket rateLimitingBucket;

    KafkaRemoteStorageManager(
        final Logger log, final Time time,
        final RemoteStorageManagerConfig config
    ) {
        super(log, time, config);

        this.storage = config.storage();
        this.fetcher = storage;
        this.uploader = storage;
        this.deleter = storage;

        this.objectKeyFactory = new ObjectKeyFactory(config.keyPrefix(), config.keyPrefixMask());
        this.customMetadataFields = config.customMetadataKeysIncluded();

        this.compressionEnabled = config.compressionEnabled();
        this.compressionHeuristic = config.compressionHeuristicEnabled();

        this.encryptionEnabled = config.encryptionEnabled();

        final RsaEncryptionProvider rsaEncryptionProvider =
            RemoteStorageManagerUtils.getRsaEncryptionProvider(config);
        this.aesEncryptionProvider = RemoteStorageManagerUtils.getAesEncryptionProvider(config);

        this.chunkSize = config.chunkSize();
        final ChunkManagerFactory chunkManagerFactory = new ChunkManagerFactory();
        chunkManagerFactory.configure(config.originals());
        this.chunkManager = chunkManagerFactory.initChunkManager(fetcher, aesEncryptionProvider);

        this.mapper = RemoteStorageManagerUtils.getObjectMapper(config, rsaEncryptionProvider);

        this.customMetadataSerde = new SegmentCustomMetadataSerde();

        this.segmentIndexesCache = new MemorySegmentIndexesCache();
        segmentIndexesCache.configure(config.fetchIndexesCacheConfigs());

        this.rateLimitingBucket = config.uploadRateLimit().stream()
            .mapToObj(RateLimitedInputStream::rateLimitBucket)
            .findFirst()
            .orElse(null);

        this.segmentManifestCache = new MemorySegmentManifestCache(config.storage(), mapper);
        this.segmentManifestCache.configure(config.segmentManifestCacheConfigs());
    }

    // For testing.
    void setChunkManager(final ChunkManager chunkManager) {
        this.chunkManager = chunkManager;
    }

    // For testing.
    void setSegmentManifestCache(final MemorySegmentManifestCache segmentManifestCache) {
        this.segmentManifestCache = segmentManifestCache;
    }

    @Override
    Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final UploadMetricReporter uploadMetricReporter
    ) throws RemoteStorageException {
        final var customMetadataBuilder =
            new SegmentCustomMetadataBuilder(customMetadataFields, objectKeyFactory, remoteLogSegmentMetadata);

        try {
            final boolean requiresCompression = requiresCompression(logSegmentData);

            final DataKeyAndAAD maybeEncryptionKey;
            if (encryptionEnabled) {
                maybeEncryptionKey = aesEncryptionProvider.createDataKeyAndAAD();
            } else {
                maybeEncryptionKey = null;
            }

            final ChunkIndex chunkIndex = uploadSegmentLog(
                remoteLogSegmentMetadata,
                logSegmentData,
                requiresCompression,
                maybeEncryptionKey,
                customMetadataBuilder,
                uploadMetricReporter
            );

            final SegmentIndexesV1 segmentIndexes = uploadIndexes(
                remoteLogSegmentMetadata,
                logSegmentData,
                maybeEncryptionKey,
                customMetadataBuilder,
                uploadMetricReporter
            );

            uploadManifest(
                remoteLogSegmentMetadata,
                chunkIndex,
                segmentIndexes,
                requiresCompression,
                maybeEncryptionKey,
                customMetadataBuilder,
                uploadMetricReporter
            );
        } catch (final Exception e) {
            try {
                // best effort on removing orphan files
                deleteSegmentObjects(remoteLogSegmentMetadata);
            } catch (final Exception ex) {
                // ignore all exceptions
                log.warn("Removing orphan files failed", ex);
            }
            throw new RemoteStorageException(e);
        }

        return buildCustomMetadata(customMetadataBuilder);
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

    ChunkIndex uploadSegmentLog(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final boolean requiresCompression,
        final DataKeyAndAAD maybeEncryptionKey,
        final SegmentCustomMetadataBuilder customMetadataBuilder,
        final UploadMetricReporter uploadMetricReporter
    ) throws IOException, StorageBackendException {
        final var objectKey = objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.LOG);

        try (final var logSegmentInputStream = Files.newInputStream(logSegmentData.logSegment())) {
            final var transformEnum = transformation(logSegmentInputStream, requiresCompression, maybeEncryptionKey);
            final TransformFinisher transformFinisher = TransformFinisher.newBuilder(
                    transformEnum,
                    remoteLogSegmentMetadata.segmentSizeInBytes()
                )
                .withRateLimitingBucket(rateLimitingBucket)
                .withOriginalFilePath(logSegmentData.logSegment())
                .build();

            try (final var sis = transformFinisher.toInputStream()) {
                final var bytes = uploader.upload(sis, objectKey);
                uploadMetricReporter.report(ObjectKeyFactory.Suffix.LOG, bytes);
                customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.LOG, bytes);

                log.debug("Uploaded segment log for {}, size: {}", remoteLogSegmentMetadata, bytes);
            }
            return transformFinisher.chunkIndex();
        }
    }

    private TransformChunkEnumeration transformation(
        final InputStream logSegmentInputStream,
        final boolean requiresCompression,
        final DataKeyAndAAD maybeEncryptionKey
    ) {
        TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
            logSegmentInputStream,
            chunkSize
        );
        if (requiresCompression) {
            transformEnum = new CompressionChunkEnumeration(transformEnum);
        }
        if (encryptionEnabled) {
            transformEnum = new EncryptionChunkEnumeration(
                transformEnum,
                () -> aesEncryptionProvider.encryptionCipher(maybeEncryptionKey)
            );
        }
        return transformEnum;
    }

    SegmentIndexesV1 uploadIndexes(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData segmentData,
        final DataKeyAndAAD maybeEncryptionKey,
        final SegmentCustomMetadataBuilder customMetadataBuilder,
        final UploadMetricReporter uploadMetricReporter
    ) throws IOException, RemoteStorageException, StorageBackendException {
        final List<InputStream> indexes = new ArrayList<>(RemoteStorageManager.IndexType.values().length);
        final SegmentIndexesV1Builder segmentIndexBuilder = new SegmentIndexesV1Builder();

        try (final ClosableInputStreamHolder closableInputStreamHolder = new ClosableInputStreamHolder()) {
            final var offsetIndex = transformIndex(
                RemoteStorageManager.IndexType.OFFSET,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.offsetIndex())),
                indexSize(segmentData.offsetIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(offsetIndex);
            final var timeIndex = transformIndex(
                RemoteStorageManager.IndexType.TIMESTAMP,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.timeIndex())),
                indexSize(segmentData.timeIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(timeIndex);
            final var producerSnapshotIndex = transformIndex(
                RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT,
                closableInputStreamHolder.add(Files.newInputStream(segmentData.producerSnapshotIndex())),
                indexSize(segmentData.producerSnapshotIndex()),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(producerSnapshotIndex);
            final var leaderEpoch = transformIndex(
                RemoteStorageManager.IndexType.LEADER_EPOCH,
                closableInputStreamHolder.add(new ByteBufferInputStream(segmentData.leaderEpochIndex())),
                segmentData.leaderEpochIndex().remaining(),
                maybeEncryptionKey,
                segmentIndexBuilder
            );
            indexes.add(leaderEpoch);
            if (segmentData.transactionIndex().isPresent()) {
                final var transactionIndex = transformIndex(
                    RemoteStorageManager.IndexType.TRANSACTION,
                    closableInputStreamHolder.add(Files.newInputStream(segmentData.transactionIndex().get())),
                    indexSize(segmentData.transactionIndex().get()),
                    maybeEncryptionKey,
                    segmentIndexBuilder
                );
                indexes.add(transactionIndex);
            }
            final var suffix = ObjectKeyFactory.Suffix.INDEXES;
            final ObjectKey key = objectKeyFactory.key(remoteLogSegmentMetadata, suffix);
            try (final var in = new SequenceInputStream(Collections.enumeration(indexes))) {
                final var bytes = uploader.upload(in, key);
                uploadMetricReporter.report(suffix, bytes);
                customMetadataBuilder.addUploadResult(suffix, bytes);

                log.debug("Uploaded indexes file for {}, size: {}", remoteLogSegmentMetadata, bytes);
            }
        }
        return segmentIndexBuilder.build();
    }

    InputStream transformIndex(final RemoteStorageManager.IndexType indexType,
                               final InputStream index,
                               final int size,
                               final DataKeyAndAAD maybeEncryptionKey,
                               final SegmentIndexesV1Builder segmentIndexBuilder) {
        log.debug("Transforming index {} with size {}", indexType, size);
        if (size > 0) {
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(index, size);
            if (encryptionEnabled) {
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    () -> aesEncryptionProvider.encryptionCipher(maybeEncryptionKey));
            }
            final var transformFinisher = TransformFinisher.newBuilder(transformEnum, size)
                .withChunkingDisabled()
                .withRateLimitingBucket(rateLimitingBucket)
                .build();
            // Getting next element and expecting that it is the only one.
            // No need to get a sequenced input stream
            final var inputStream = transformFinisher.nextElement();
            final var chunkIndex = transformFinisher.chunkIndex();
            // by getting a chunk index, means that the transformation is completed.
            if (chunkIndex == null) {
                throw new IllegalStateException("Chunking disabled when single chunk is expected");
            }
            if (chunkIndex.chunks().size() != 1) {
                // not expected, as next element run once. But for safety
                throw new IllegalStateException("Number of chunks different than 1, single chunk is expected");
            }
            segmentIndexBuilder.add(indexType, chunkIndex.chunks().get(0).range().size());
            return inputStream;
        } else {
            segmentIndexBuilder.add(indexType, 0);
            return InputStream.nullInputStream();
        }
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

    void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                        final ChunkIndex chunkIndex,
                        final SegmentIndexesV1 segmentIndexes,
                        final boolean requiresCompression,
                        final DataKeyAndAAD maybeEncryptionKey,
                        final SegmentCustomMetadataBuilder customMetadataBuilder,
                        final UploadMetricReporter uploadMetricReporter
    ) throws StorageBackendException, IOException {
        final SegmentEncryptionMetadataV1 maybeEncryptionMetadata;
        if (maybeEncryptionKey != null) {
            maybeEncryptionMetadata = new SegmentEncryptionMetadataV1(maybeEncryptionKey);
        } else {
            maybeEncryptionMetadata = null;
        }
        final SegmentManifest segmentManifest = new SegmentManifestV1(
            chunkIndex,
            segmentIndexes,
            requiresCompression,
            maybeEncryptionMetadata,
            remoteLogSegmentMetadata
        );
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final ObjectKey manifestObjectKey =
            objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);

        try (final ByteArrayInputStream manifestContent = new ByteArrayInputStream(manifest.getBytes())) {
            final var bytes = uploader.upload(manifestContent, manifestObjectKey);
            uploadMetricReporter.report(ObjectKeyFactory.Suffix.MANIFEST, bytes);
            customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, bytes);

            log.debug("Uploaded segment manifest for {}, size: {}", remoteLogSegmentMetadata, bytes);
        }
    }

    @Override
    InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final BytesRange range
    ) throws RemoteStorageException, SegmentManifestNotFoundException {
        try {
            final SegmentManifest segmentManifest;
            try {
                final ObjectKey manifestKey =
                    objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);
                segmentManifest = segmentManifestCache.get(manifestKey);
            } catch (final KeyNotFoundException | KeyNotFoundRuntimeException e) {
                throw new SegmentManifestNotFoundException(e);
            }

            final var suffix = ObjectKeyFactory.Suffix.LOG;
            final var segmentKey = objectKey(remoteLogSegmentMetadata, suffix);
            return new FetchChunkEnumeration(chunkManager, segmentKey, segmentManifest, range)
                .toInputStream();
        } catch (final SegmentManifestNotFoundException e) {
            // This exception has meaning up the call stack, pass it as is.
            throw e;
        } catch (final ClosedByInterruptException e) {
            log.debug("Fetching log segment {} with range {} was interrupted", remoteLogSegmentMetadata, range);
            return InputStream.nullInputStream();
        } catch (final KeyNotFoundRuntimeException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final RuntimeException | StorageBackendException e) {
            return returnNullInputStreamIfInterruptedElseThrow(
                e, remoteLogSegmentMetadata,
                () -> log.debug(
                    "Fetching log segment {} with range {} was interrupted", remoteLogSegmentMetadata, range)
            );
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    InputStream fetchIndex(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final RemoteStorageManager.IndexType indexType
    ) throws RemoteStorageException, SegmentManifestNotFoundException {
        try {
            final SegmentManifest segmentManifest;
            try {
                final ObjectKey manifestKey =
                    objectKeyFactory.key(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.MANIFEST);
                segmentManifest = segmentManifestCache.get(manifestKey);
            } catch (final KeyNotFoundException | KeyNotFoundRuntimeException e) {
                throw new SegmentManifestNotFoundException(e);
            }

            final var key = objectKey(remoteLogSegmentMetadata, ObjectKeyFactory.Suffix.INDEXES);
            final var segmentIndex = segmentManifest.segmentIndexes().segmentIndex(indexType);
            if (segmentIndex == null) {
                throw new RemoteResourceNotFoundException("Index " + indexType + " not found on " + key);
            }
            if (segmentIndex.range().isEmpty()) {
                return InputStream.nullInputStream();
            }
            return segmentIndexesCache.get(
                key,
                indexType,
                () -> fetchIndexBytes(key, segmentIndex, segmentManifest)
            );
        } catch (final SegmentManifestNotFoundException | RemoteResourceNotFoundException e) {
            // These exceptions have meaning up the call stack, pass them as is.
            throw e;
        } catch (final ClosedByInterruptException e) {
            log.debug("Fetching log index {} from {} was interrupted", indexType, remoteLogSegmentMetadata);
            return InputStream.nullInputStream();
        } catch (final KeyNotFoundException e) {
            throw new RemoteResourceNotFoundException(e);
        } catch (final RuntimeException | StorageBackendException e) {
            return returnNullInputStreamIfInterruptedElseThrow(
                e, remoteLogSegmentMetadata,
                () -> log.debug("Fetching log index {} from {} was interrupted", indexType, remoteLogSegmentMetadata)
            );
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    static InputStream returnNullInputStreamIfInterruptedElseThrow(
        final Throwable exception,
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final Runnable log
    ) throws RemoteStorageException {
        if (exception.getCause() instanceof InterruptedException
            || exception.getCause() instanceof ClosedByInterruptException) {
            log.run();
            return InputStream.nullInputStream();
        } else if (exception.getCause() instanceof StorageBackendException) {
            return returnNullInputStreamIfInterruptedElseThrow(exception.getCause(), remoteLogSegmentMetadata, log);
        } else {
            throw new RemoteStorageException(exception);
        }
    }

    private byte[] fetchIndexBytes(
        final ObjectKey key,
        final SegmentIndex segmentIndex,
        final SegmentManifest segmentManifest
    ) {
        final InputStream in;
        try {
            in = fetcher.fetch(key, segmentIndex.range());
        } catch (final StorageBackendException e) {
            throw new RuntimeException("Error fetching index from remote storage", e);
        }

        DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(in);
        final Optional<SegmentEncryptionMetadata> encryptionMetadata = segmentManifest.encryption();
        if (encryptionMetadata.isPresent()) {
            detransformEnum = new DecryptionChunkEnumeration(
                detransformEnum,
                encryptionMetadata.get().ivSize(),
                encryptedChunk -> aesEncryptionProvider.decryptionCipher(encryptedChunk,
                    encryptionMetadata.get())
            );
        }
        final var detransformFinisher = new DetransformFinisher(detransformEnum);
        try (final var is = detransformFinisher.toInputStream()) {
            return is.readAllBytes();
        } catch (final IOException e) {
            throw new RuntimeException("Error reading de-transformed index bytes", e);
        }
    }

    private Optional<RemoteLogSegmentMetadata.CustomMetadata> buildCustomMetadata(
        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) {
        final var customFields = customMetadataBuilder.build();
        if (!customFields.isEmpty()) {
            final var customMetadataBytes = customMetadataSerde.serialize(customFields);
            return Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(customMetadataBytes));
        } else {
            return Optional.empty();
        }
    }

    @Override
    void deleteLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata
    ) throws RemoteStorageException {
        try {
            deleteSegmentObjects(remoteLogSegmentMetadata);
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private void deleteSegmentObjects(final RemoteLogSegmentMetadata metadata) throws StorageBackendException {
        final Set<ObjectKey> keys = Arrays.stream(ObjectKeyFactory.Suffix.values())
            .map(s -> objectKeyFactory.key(metadata, s))
            .collect(Collectors.toSet());
        deleter.delete(keys);
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

    @Override
    public void close() throws IOException {
        storage.close();
    }
}
