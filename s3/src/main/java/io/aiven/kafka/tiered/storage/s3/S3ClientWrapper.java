/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tiered.storage.s3;

import javax.crypto.SecretKey;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.commons.io.CryptoIOProvider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tiered.storage.s3.S3StorageUtils.getFileKey;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

public class S3ClientWrapper {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);
    private static final String LOG_FILE_SUFFIX = "log";
    private static final String METADATA_FILE_SUFFIX = "metadata.json";
    private final AmazonS3 s3Client;
    private final S3RemoteStorageManagerConfig config;
    private final S3EncryptionKeyProvider s3EncryptionKeyProvider;

    public S3ClientWrapper(final S3RemoteStorageManagerConfig config,
                           final AmazonS3 s3Client,
                           final S3EncryptionKeyProvider s3EncryptionKeyProvider) {
        this.config = config;
        this.s3Client = s3Client;
        this.s3EncryptionKeyProvider = s3EncryptionKeyProvider;
    }

    public InputStream fetchLogFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                    final int startPosition,
                                    final int endPosition) throws RemoteStorageException {
        final String logFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), LOG_FILE_SUFFIX);

        final int firstPart = startPosition / config.s3StorageUploadPartSize();
        final int lastPart;
        if (remoteLogSegmentMetadata.segmentSizeInBytes() % config.s3StorageUploadPartSize() == 0) {
            lastPart = endPosition / config.s3StorageUploadPartSize();
        } else {
            lastPart = endPosition / config.s3StorageUploadPartSize() + 1;
        }
        final String metadataFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), METADATA_FILE_SUFFIX);
        final SecretKey encryptionKey = s3EncryptionKeyProvider.createOrRestoreEncryptionKey(metadataFileKey);
        final byte[] aad = s3EncryptionKeyProvider.createOrRestoreAAD(metadataFileKey);
        final CryptoIOProvider cryptoIOProvider = new CryptoIOProvider(encryptionKey, aad, config.ioBufferSize());
        final ArrayList<InputStream> inputStreams = new ArrayList<>();
        for (int i = firstPart; i < lastPart - 1; i++) {
            final String key = logFileKey + "_" + i;
            try {
                final GetObjectRequest getObjectRequest = new GetObjectRequest(config.s3BucketName(), key);
                final S3Object s3Object = s3Client.getObject(getObjectRequest);
                inputStreams.add(cryptoIOProvider.decryptAndDecompress(s3Object.getObjectContent()));
            } catch (final Exception e) {
                throw new RemoteStorageException(String.format("Error fetching log segment data from %s", key), e);
            }
        }
        final int end = endPosition % config.s3StorageUploadPartSize();
        final String key = logFileKey + "_" + (lastPart - 1);
        try {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(config.s3BucketName(), key);
            final S3Object s3Object = s3Client.getObject(getObjectRequest);
            final BoundedInputStream boundedInputStream =
                    new BoundedInputStream(cryptoIOProvider.decryptAndDecompress(s3Object.getObjectContent()), end);
            inputStreams.add(boundedInputStream);
        } catch (final IOException e) {
            throw new RemoteStorageException(String.format("Error fetching log segment data from %s", key), e);
        }
        return new SequenceInputStream(Collections.enumeration(inputStreams));
    }

    public void uploadLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                     final LogSegmentData logSegmentData) throws RemoteStorageException {

        final String logFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), LOG_FILE_SUFFIX);
        final String offsetIndexFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), OFFSET.name());
        final String timeIndexFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), TIMESTAMP.name());
        final String producerSnapshotFileKey =
            getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), PRODUCER_SNAPSHOT.name());
        final String transactionIndexFileKey =
            getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), TRANSACTION.name());
        final String leaderEpochIndexFileKey =
            getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), LEADER_EPOCH.name());
        final String metadataFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), METADATA_FILE_SUFFIX);
        final SecretKey encryptionKey = s3EncryptionKeyProvider.createOrRestoreEncryptionKey(metadataFileKey);
        final byte[] aad = s3EncryptionKeyProvider.createOrRestoreAAD(metadataFileKey);
        final CryptoIOProvider cryptoIOProvider = new CryptoIOProvider(encryptionKey, aad, config.ioBufferSize());
        try {
            log.debug("Uploading log file: {}", logFileKey);
            uploadLogFile(cryptoIOProvider, logFileKey, logSegmentData.logSegment());

            log.debug("Uploading offset index file: {}", offsetIndexFileKey);
            uploadIndex(cryptoIOProvider, offsetIndexFileKey, logSegmentData.offsetIndex());

            log.debug("Uploading time index file: {}", timeIndexFileKey);
            uploadIndex(cryptoIOProvider, timeIndexFileKey, logSegmentData.timeIndex());

            log.debug("Uploading producer snapshot file: {}", producerSnapshotFileKey);
            uploadIndex(cryptoIOProvider, producerSnapshotFileKey, logSegmentData.producerSnapshotIndex());

            if (logSegmentData.transactionIndex().isPresent()) {
                log.debug("Uploading transaction file: {}", transactionIndexFileKey);
                uploadIndex(cryptoIOProvider, transactionIndexFileKey, logSegmentData.transactionIndex().get());
            }

            log.debug("Uploading leader epoch file: {}", leaderEpochIndexFileKey);
            uploadIndex(cryptoIOProvider, leaderEpochIndexFileKey, logSegmentData.leaderEpochIndex());
        } catch (final IOException e) {
            final String message = String.format("Error uploading remote log segment %s", remoteLogSegmentMetadata);
            log.error(message, e);

            log.info("Attempt to clean up partial upload");
            try {
                deleteSegmentData(remoteLogSegmentMetadata);
            } catch (final RuntimeException ex) {
                log.error("Error cleaning up uploaded files", ex);
            }
            throw new RemoteStorageException(message, e);
        }
    }

    public void uploadLogFile(final CryptoIOProvider cryptoIOProvider,
                              final String logFileKey,
                              final Path filePath) throws IOException {

        long position = 0L;
        int partNumber = 0;


        final byte[] buffer = new byte[config.s3StorageUploadPartSize()];
        final File logFile = filePath.toFile();
        final long length = logFile.length();
        int read;
        try (final FileInputStream inputStream = new FileInputStream(logFile)) {
            while (position <= length && (read = inputStream.read(buffer)) != -1) {
                final long uploaded = cryptoIOProvider.compressAndEncrypt(
                        new ByteArrayInputStream(buffer, 0, read),
                        new S3OutputStream(config.s3BucketName(), logFileKey + "_" + partNumber,
                                config.multiPartUploadPartSize(), s3Client)
                );
                position += uploaded;
                partNumber++;
            }
        }
        if (position != length) {
            throw new RuntimeException("failed to upload " + position + ":" + length);
        }
    }

    public void uploadIndex(final CryptoIOProvider cryptoIOProvider,
                            final String logFileKey,
                            final Path filePath) throws IOException {

        try (final FileInputStream inputStream = new FileInputStream(filePath.toFile())) {
            cryptoIOProvider.compressAndEncrypt(
                    inputStream,
                    new S3OutputStream(config.s3BucketName(), logFileKey, config.multiPartUploadPartSize(), s3Client)
            );
        }
    }

    public void uploadIndex(final CryptoIOProvider cryptoIOProvider,
                            final String logFileKey,
                            final ByteBuffer data) throws IOException {

        try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(data.array())) {
            cryptoIOProvider.compressAndEncrypt(
                    inputStream,
                    new S3OutputStream(config.s3BucketName(), logFileKey, config.multiPartUploadPartSize(), s3Client)
            );
        }
    }

    public InputStream fetchIndexFile(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                      final RemoteStorageManager.IndexType indexType) throws RemoteStorageException {

        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String indexFileKey = getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), indexType.name());
        final List<S3ObjectSummary> objectSummaries =
                s3Client.listObjects(config.s3BucketName(), indexFileKey).getObjectSummaries();
        if (objectSummaries.size() == 1) {
            final GetObjectRequest getObjectRequest =
                    new GetObjectRequest(config.s3BucketName(), objectSummaries.get(0).getKey());
            final String metadataFileKey =
                getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), METADATA_FILE_SUFFIX);
            final SecretKey encryptionKey = s3EncryptionKeyProvider.createOrRestoreEncryptionKey(metadataFileKey);
            final byte[] aad = s3EncryptionKeyProvider.createOrRestoreAAD(metadataFileKey);
            final CryptoIOProvider cryptoIOProvider = new CryptoIOProvider(encryptionKey, aad, config.ioBufferSize());
            final S3Object s3Object = s3Client.getObject(getObjectRequest);
            try {
                return cryptoIOProvider.decryptAndDecompress(s3Object.getObjectContent());
            } catch (final IOException e) {
                throw new RemoteStorageException(String.format("Error fetching index file %s", indexFileKey), e);
            }
        } else {
            if (objectSummaries.size() == 0 && indexType == TRANSACTION) {
                return InputStream.nullInputStream();
            }
            throw new RemoteResourceNotFoundException(String.format("Failed to find file for key %s", indexFileKey));
        }
    }

    public void deleteSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final List<String> filesToDelete =
                s3Client.listObjects(config.s3BucketName(), getFileKey(remoteLogSegmentMetadata, config.s3Prefix(), ""))
                        .getObjectSummaries()
                        .stream()
                        .map(S3ObjectSummary::getKey)
                        .collect(Collectors.toList());

        final List<DeleteObjectsRequest.KeyVersion> keys =
                filesToDelete.stream().map(DeleteObjectsRequest.KeyVersion::new).collect(Collectors.toList());
        final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(config.s3BucketName())
                .withKeys(keys);
        s3Client.deleteObjects(deleteObjectsRequest);
    }
}
