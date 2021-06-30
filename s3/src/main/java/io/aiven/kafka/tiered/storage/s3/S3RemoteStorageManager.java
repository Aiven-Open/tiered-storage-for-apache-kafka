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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.services.s3.internal.Mimetypes.MIMETYPE_OCTET_STREAM;

/**
 * AWS S3 RemoteStorageManager.
 * Stores files as {@code {topic}-{partition}/00000000000000000123.{log|index|timeindex}.{uuid}}.
 */
public class S3RemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    private static final String LOG_FILE_SUFFIX = ".log";
    private static final String INDEX_FILE_SUFFIX = ".index";
    private static final String TIME_INDEX_FILE_SUFFIX = ".timeindex";
    private static final String PRODUCER_SNAPSHOT_FILE_SUFFIX = ".snapshot";
    private static final String TRANSACTION_INDEX_FILE_SUFFIX = ".txnindex";
    private static final String LEADER_EPOCH_INDEX_FILE_SUFFIX = ".leader-epoch-checkpoint";
    private static final int DEFAULT_PART_SIZE = 8_192;

    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private String bucket;
    private AmazonS3 s3Client;
    private TransferManager transferManager;

    public S3RemoteStorageManager() {
    }


    // for testing
    S3RemoteStorageManager(final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
        Objects.requireNonNull(endpointConfiguration, "endpointConfiguration must not be null");
        this.endpointConfiguration = endpointConfiguration;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");

        final S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(configs);
        this.bucket = config.s3BucketName();

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (this.endpointConfiguration == null) {
            s3ClientBuilder = s3ClientBuilder.withRegion(config.s3Region());
        } else {
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }

        // It's fine to pass null in here.
        s3ClientBuilder.setCredentials(config.awsCredentialsProvider());

        s3Client = s3ClientBuilder.build();
        transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        final long baseOffset = offsetFromFileName(logSegmentData.logSegment().getFileName().toString());
        final RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        final String logFileKey = getFileKey(remoteLogSegmentId, baseOffset, LOG_FILE_SUFFIX);
        final String offsetIndexFileKey = getFileKey(remoteLogSegmentId, baseOffset, INDEX_FILE_SUFFIX);
        final String timeIndexFileKey = getFileKey(remoteLogSegmentId, baseOffset, TIME_INDEX_FILE_SUFFIX);
        final String producerSnapshotFileKey =
                getFileKey(remoteLogSegmentId, baseOffset, PRODUCER_SNAPSHOT_FILE_SUFFIX);
        final String transactionIndexFileKey =
                getFileKey(remoteLogSegmentId, baseOffset, TRANSACTION_INDEX_FILE_SUFFIX);
        final String leaderEpochIndexFileKey =
                getFileKey(remoteLogSegmentId, baseOffset, LEADER_EPOCH_INDEX_FILE_SUFFIX);
        try {
            log.debug("Uploading log file: {}", logFileKey);
            final Upload logFileUpload =
                    transferManager.upload(this.bucket, logFileKey, logSegmentData.logSegment().toFile());

            log.debug("Uploading offset index file: {}", offsetIndexFileKey);
            final Upload offsetIndexFileUpload =
                    transferManager.upload(this.bucket, offsetIndexFileKey, logSegmentData.offsetIndex().toFile());

            log.debug("Uploading time index file: {}", timeIndexFileKey);
            final Upload timeIndexFileUpload =
                    transferManager.upload(this.bucket, timeIndexFileKey, logSegmentData.timeIndex().toFile());

            log.debug("Uploading producer snapshot file: {}", producerSnapshotFileKey);
            final Upload producerSnapshotFileUpload =
                    transferManager.upload(this.bucket, producerSnapshotFileKey,
                            logSegmentData.producerSnapshotIndex().toFile());

            final Optional<Upload> transactionIndexFileUpload = logSegmentData.transactionIndex().map(path -> {
                log.debug("Uploading transaction index file: {}", transactionIndexFileKey);
                return transferManager.upload(this.bucket, transactionIndexFileKey, path.toFile());
            });


            log.debug("Uploading leader epoch file: {}", leaderEpochIndexFileKey);
            final byte[] leaderEpochIndex = logSegmentData.leaderEpochIndex().array();
            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(leaderEpochIndex.length);
            objectMetadata.setContentType(MIMETYPE_OCTET_STREAM);
            final Upload leaderEpochIndexFileUpload =
                    transferManager.upload(this.bucket, leaderEpochIndexFileKey,
                            new ByteArrayInputStream(leaderEpochIndex), objectMetadata);

            logFileUpload.waitForUploadResult();
            offsetIndexFileUpload.waitForUploadResult();
            timeIndexFileUpload.waitForUploadResult();
            producerSnapshotFileUpload.waitForUploadResult();
            if (transactionIndexFileUpload.isPresent()) {
                transactionIndexFileUpload.get().waitForUploadResult();
            }
            leaderEpochIndexFileUpload.waitForUploadResult();

        } catch (final Exception e) {
            final String message = "Error uploading remote log segment " + remoteLogSegmentMetadata;
            log.error(message, e);

            log.info("Attempt to clean up partial upload");
            try {
                final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(this.bucket)
                        .withKeys(logFileKey, offsetIndexFileKey, timeIndexFileKey);
                s3Client.deleteObjects(deleteObjectsRequest);
            } catch (final Exception ex) {
                log.error("Error cleaning up uploaded files", ex);
            }

            throw new RemoteStorageException(message, e);
        }
    }

    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition,
            final int endPosition) throws RemoteStorageException {

        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be non-negative");
        }

        if (endPosition < startPosition) {
            throw new IllegalArgumentException("endPosition must >= startPosition");
        }

        final String logFileKey =
                getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset(),
                        LOG_FILE_SUFFIX);

        try {
            final GetObjectRequest getObjectRequest =
                    new GetObjectRequest(bucket, logFileKey).withRange(startPosition, endPosition);
            final S3Object s3Object = s3Client.getObject(getObjectRequest);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching log segment data from " + logFileKey, e);
        }
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be non-negative");
        }

        final String logFileKey =
                getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset(),
                        LOG_FILE_SUFFIX);

        try {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, logFileKey).withRange(startPosition);
            final S3Object s3Object = s3Client.getObject(getObjectRequest);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching log segment data from " + logFileKey, e);
        }
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final IndexType indexType)
            throws RemoteStorageException {
        switch (indexType) {
            case OFFSET:
                return fetchIndex(remoteLogSegmentMetadata, INDEX_FILE_SUFFIX);
            case TIMESTAMP:
                return fetchIndex(remoteLogSegmentMetadata, TIME_INDEX_FILE_SUFFIX);
            case TRANSACTION:
                return fetchIndex(remoteLogSegmentMetadata, TRANSACTION_INDEX_FILE_SUFFIX);
            case PRODUCER_SNAPSHOT:
                return fetchIndex(remoteLogSegmentMetadata, PRODUCER_SNAPSHOT_FILE_SUFFIX);
            case LEADER_EPOCH:
                return fetchIndex(remoteLogSegmentMetadata, LEADER_EPOCH_INDEX_FILE_SUFFIX);
            default:
                throw new RemoteStorageException("Index type unsupported");
        }
    }

    private InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final String indexFileSuffix)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String offsetIndexFileKey = getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(),
                remoteLogSegmentMetadata.startOffset(), indexFileSuffix);

        try {
            final S3Object s3Object = s3Client.getObject(bucket, offsetIndexFileKey);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching index for " + offsetIndexFileKey, e);
        }
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String logFileKey =
                getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset(),
                        LOG_FILE_SUFFIX);
        final String offsetIndexFileKey = getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(),
                remoteLogSegmentMetadata.startOffset(), INDEX_FILE_SUFFIX);
        final String timeIndexFileKey =
                getFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset(),
                        TIME_INDEX_FILE_SUFFIX);

        try {
            final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket)
                    .withKeys(logFileKey, offsetIndexFileKey, timeIndexFileKey);
            s3Client.deleteObjects(deleteObjectsRequest);
        } catch (final Exception e) {
            throw new RemoteStorageException(
                    String.format("Error deleting %s, %s or %s", logFileKey, offsetIndexFileKey, timeIndexFileKey), e);
        }
    }

    @Override
    public void close() {
        if (transferManager != null) {
            transferManager.shutdownNow(); // shuts down the S3 client too
        }
    }

    private String getFileKey(final RemoteLogSegmentId remoteLogSegmentId, final long fileNameBaseOffset,
                              final String suffix) {
        return fileNamePrefix(remoteLogSegmentId) + filenamePrefixFromOffset(fileNameBaseOffset)
                + suffix + "." + remoteLogSegmentId.id();
    }

    private String fileNamePrefix(final RemoteLogSegmentId remoteLogSegmentId) {
        return remoteLogSegmentId.topicIdPartition().topicPartition().toString() + "/";
    }


    /**
     * Parses a log segment file name and extracts the offset from it.
     *
     * @implNote Taken from kafka.log.Log.offsetFromFileName
     */
    private static long offsetFromFileName(final String filename) {
        return Long.parseLong(filename.substring(0, filename.indexOf('.')));
    }

    /**
     * Make the log segment file name from an offset.
     *
     * @implNote Taken from kafka.log.Log.filenamePrefixFromOffset
     */
    static String filenamePrefixFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
}
