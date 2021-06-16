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

import java.io.InputStream;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;

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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS S3 RemoteStorageManager.
 * Stores files as {@code {topic}-{partition}/00000000000000000123.{log|index|timeindex}.{uuid}}.
 */
public class S3RemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    private static final String LOG_FILE_SUFFIX = ".log";
    private static final String INDEX_FILE_SUFFIX = ".index";
    private static final String TIME_INDEX_FILE_SUFFIX = ".timeindex";

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
        final String logFileKey = logFileKey(remoteLogSegmentId, baseOffset);
        final String offsetIndexFileKey = offsetIndexFileKey(remoteLogSegmentId, baseOffset);
        final String timeIndexFileKey = timeIndexFileKey(remoteLogSegmentId, baseOffset);
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

            logFileUpload.waitForUploadResult();
            offsetIndexFileUpload.waitForUploadResult();
            timeIndexFileUpload.waitForUploadResult();

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
                logFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset());

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
                logFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset());

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
                return fetchOffsetIndex(remoteLogSegmentMetadata);
            case TIMESTAMP:
                return fetchTimestampIndex(remoteLogSegmentMetadata);
            default:
                throw new RemoteStorageException("Index type unsupported");
        }
    }

    public InputStream fetchOffsetIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {

        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String offsetIndexFileKey = offsetIndexFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(),
                remoteLogSegmentMetadata.startOffset());

        try {
            final S3Object s3Object = s3Client.getObject(bucket, offsetIndexFileKey);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching offset index from " + offsetIndexFileKey, e);
        }
    }

    public InputStream fetchTimestampIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String timeIndexFileKey =
                timeIndexFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset());

        try {
            final S3Object s3Object = s3Client.getObject(bucket, timeIndexFileKey);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching timestamp index from " + timeIndexFileKey, e);
        }
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final String logFileKey =
                logFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset());
        final String offsetIndexFileKey = offsetIndexFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(),
                remoteLogSegmentMetadata.startOffset());
        final String timeIndexFileKey =
                timeIndexFileKey(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata.startOffset());

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

    private String logFileKey(final RemoteLogSegmentId remoteLogSegmentId, final long fileNameBaseOffset) {
        return fileNamePrefix(remoteLogSegmentId) + filenamePrefixFromOffset(fileNameBaseOffset)
                + LOG_FILE_SUFFIX + "." + remoteLogSegmentId.id();
    }

    private String offsetIndexFileKey(final RemoteLogSegmentId remoteLogSegmentId, final long fileNameBaseOffset) {
        return fileNamePrefix(remoteLogSegmentId) + filenamePrefixFromOffset(fileNameBaseOffset)
                + INDEX_FILE_SUFFIX + "." + remoteLogSegmentId.id();
    }

    private String timeIndexFileKey(final RemoteLogSegmentId remoteLogSegmentId, final long fileNameBaseOffset) {
        return fileNamePrefix(remoteLogSegmentId) + filenamePrefixFromOffset(fileNameBaseOffset)
                + TIME_INDEX_FILE_SUFFIX + "." + remoteLogSegmentId.id();
    }

    private String fileNamePrefix(final RemoteLogSegmentId remoteLogSegmentId) {
        return remoteLogSegmentId.topicIdPartition().topicPartition().toString() + "/";
    }


    /**
     * Parses a log segment file name and extracts the offset from it.
     * @implNote Taken from kafka.log.Log.offsetFromFileName
     */
    private static long offsetFromFileName(final String filename) {
        return Long.parseLong(filename.substring(0, filename.indexOf('.')));
    }

    /**
     * Make the log segment file name from an offset.
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
