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

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadataV1;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeyDeserializer;
import io.aiven.kafka.tieredstorage.commons.manifest.serde.DataKeySerializer;
import io.aiven.kafka.tieredstorage.commons.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;
import io.aiven.kafka.tieredstorage.commons.transform.BaseTransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.CompressionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.EncryptionChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.FetchChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.TransformChunkEnumeration;
import io.aiven.kafka.tieredstorage.commons.transform.TransformFinisher;

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

public class UniversalRemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(UniversalRemoteStorageManager.class);

    private final Metrics metrics;
    private final Sensor segmentCopyPerSec;

    private UniversalRemoteStorageManagerConfig config;

    private ObjectStorageFactory objectStorageFactory;
    private boolean compression;
    private boolean encryption;
    private int chunkSize;
    private RsaEncryptionProvider rsaEncryptionProvider;
    private AesEncryptionProvider aesEncryptionProvider;
    private ObjectMapper mapper;
    private ChunkManager chunkManager;
    private ObjectKey objectKey;

    UniversalRemoteStorageManager() {
        this(Time.SYSTEM);
    }

    // for testing
    UniversalRemoteStorageManager(final Time time) {
        final JmxReporter reporter = new JmxReporter();
        metrics = new Metrics(
            new MetricConfig(), List.of(reporter), time,
            new KafkaMetricsContext("aiven.kafka.server.tieredstorage")
        );
        segmentCopyPerSec = metrics.sensor("segment-copy");
        segmentCopyPerSec.add(
            metrics.metricName("segment-copy-rate", "remote-storage-manager-metrics"), new Rate());
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        config = new UniversalRemoteStorageManagerConfig(configs);
        objectStorageFactory = config.objectStorageFactory();
        objectKey = new ObjectKey(config.keyPrefix());
        encryption = config.encryptionEnabled();
        if (encryption) {
            rsaEncryptionProvider = RsaEncryptionProvider.of(
                config.encryptionPublicKeyFile(),
                config.encryptionPrivateKeyFile()
            );
            aesEncryptionProvider = new AesEncryptionProvider();
        }
        chunkManager = new ChunkManager(
            objectStorageFactory.fileFetcher(),
            objectKey,
            aesEncryptionProvider,
            config.chunkCache()
        );

        chunkSize = config.chunkSize();
        compression = config.compressionEnabled();

        mapper = getObjectMapper();
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        if (encryption) {
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

        segmentCopyPerSec.record();

        try {
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(
                Files.newInputStream(logSegmentData.logSegment()), chunkSize);
            SegmentEncryptionMetadataV1 encryptionMetadata = null;
            if (compression) {
                transformEnum = new CompressionChunkEnumeration(transformEnum);
            }
            if (encryption) {
                final DataKeyAndAAD dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    () -> aesEncryptionProvider.encryptionCipher(dataKeyAndAAD));
                encryptionMetadata = new SegmentEncryptionMetadataV1(dataKeyAndAAD.dataKey, dataKeyAndAAD.aad);
            }
            final var transformFinisher =
                new TransformFinisher(transformEnum, remoteLogSegmentMetadata.segmentSizeInBytes());
            try (final var sis = new SequenceInputStream(transformFinisher)) {
                final String fileKey =
                    objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
                objectStorageFactory.fileUploader().upload(sis, fileKey);
            }


            final ChunkIndex chunkIndex = transformFinisher.chunkIndex();
            final SegmentManifest segmentManifest =
                new SegmentManifestV1(chunkIndex, compression, encryptionMetadata);
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
        } catch (final StorageBackEndException | IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    private void uploadIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final InputStream index,
                                 final IndexType indexType) throws StorageBackEndException {
        objectStorageFactory.fileUploader().upload(index,
            objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
    }

    private void uploadManifest(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                final SegmentManifest segmentManifest)
        throws StorageBackEndException, IOException {
        final String manifest = mapper.writeValueAsString(segmentManifest);
        final String manifestFileKey =
            objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST);

        objectStorageFactory.fileUploader().upload(new ByteArrayInputStream(manifest.getBytes()), manifestFileKey);
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

            final SegmentManifestV1 segmentManifestV1 = mapper.readValue(manifest, SegmentManifestV1.class);
            final FetchChunkEnumeration fetchChunkEnumeration = new FetchChunkEnumeration(
                chunkManager,
                remoteLogSegmentMetadata,
                segmentManifestV1,
                startPosition,
                endPosition);
            return new SequenceInputStream(fetchChunkEnumeration);
        } catch (final StorageBackEndException | IOException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        try {
            return objectStorageFactory.fileFetcher()
                .fetch(objectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.fromIndexType(indexType)));
        } catch (final StorageBackEndException e) {
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
                objectStorageFactory.fileDeleter()
                    .delete(objectKey.key(remoteLogSegmentMetadata, suffix));
            }
        } catch (final StorageBackEndException e) {
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public void close() {
        try {
            metrics.close();
        } catch (final Exception e) {
            log.warn("Error while closing metrics", e);
        }
    }
}
