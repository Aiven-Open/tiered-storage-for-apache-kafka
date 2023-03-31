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
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS S3 RemoteStorageManager.
 * Stores files as {@code {topic}-{partition}/{uuid}-00000000000000000123.{suffix}}.
 */
public class S3RemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);
    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private S3RemoteStorageManagerConfig config;

    private S3ClientWrapper s3ClientWrapper;

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
        config = new S3RemoteStorageManagerConfig(configs);
        final AmazonS3 s3Client = initS3Client(endpointConfiguration);
        final S3EncryptionKeyProvider s3EncryptionKeyProvider = new S3EncryptionKeyProvider(s3Client, config);
        s3ClientWrapper = new S3ClientWrapper(config, s3Client, s3EncryptionKeyProvider);
    }

    private AmazonS3 initS3Client(final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (endpointConfiguration == null) {
            if (config.s3EndpointUrl().isEmpty()) {
                s3ClientBuilder = s3ClientBuilder.withRegion(config.s3Region());
            } else {
                final AwsClientBuilder.EndpointConfiguration endpointConfig =
                    new AwsClientBuilder.EndpointConfiguration(
                        config.s3EndpointUrl().get(),
                        config.s3Region().getName());
                s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfig);
            }
        } else {
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }
        s3ClientBuilder.setCredentials(config.awsCredentialsProvider());
        return s3ClientBuilder.build();
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");
        s3ClientWrapper.uploadLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
    }

    @Override
    public InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final int startPosition,
        final int endPosition) throws RemoteStorageException {
        log.info("Fetching log segment from remote storage start position: {}, end position {}.", startPosition,
            endPosition);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be non-negative");
        }

        if (endPosition < startPosition) {
            throw new IllegalArgumentException("endPosition must >= startPosition");
        }
        return s3ClientWrapper.fetchLogFile(remoteLogSegmentMetadata, startPosition, endPosition);
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        log.info("Fetching log segment from remote storage starting from position {}.",
            remoteLogSegmentMetadata.startOffset());
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be non-negative");
        }
        final int endPosition = remoteLogSegmentMetadata.segmentSizeInBytes();

        return s3ClientWrapper.fetchLogFile(remoteLogSegmentMetadata, startPosition, endPosition);
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final IndexType indexType)
        throws RemoteStorageException {
        log.info("Fetching {} index from remote storage for segment with offset {}.", indexType,
            remoteLogSegmentMetadata.startOffset());
        return s3ClientWrapper.fetchIndexFile(remoteLogSegmentMetadata, indexType);
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        log.info("Deleting log segment from remote storage for offset {}", remoteLogSegmentMetadata.startOffset());
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        try {
            s3ClientWrapper.deleteSegmentData(remoteLogSegmentMetadata);
        } catch (final Exception e) {
            throw new RemoteStorageException(
                String.format("Error deleting files for %s ", remoteLogSegmentMetadata.remoteLogSegmentId().id()),
                e);
        }
    }

    @Override
    public void close() {
    }
}
