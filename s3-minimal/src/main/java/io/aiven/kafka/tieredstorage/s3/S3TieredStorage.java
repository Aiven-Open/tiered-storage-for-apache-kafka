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

package io.aiven.kafka.tieredstorage.s3;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3TieredStorage implements RemoteStorageManager {

    private static final Logger LOG = LoggerFactory.getLogger(S3TieredStorage.class);

    public static final String SEGMENT_SUFFIX = ".log";
    public static final String OFFSET_INDEX_SUFFIX = "_offset.index";
    public static final String TIME_INDEX_SUFFIX = "_time.index";
    public static final String TX_INDEX_SUFFIX = "_tx.index";
    public static final String PRODUCER_SNAPSHOT_SUFFIX = "_producer.snapshot";
    public static final String LEADER_EPOCH_SUFFIX = "_leader.epoch";

    private AmazonS3 s3Client;
    private String bucketName;
    private int fetchMaxBytes;
    private String objectPrefix;

    @Override
    public void configure(final Map<String, ?> configs) {
        final S3TieredStorageConfig config = new S3TieredStorageConfig(configs);
        s3Client = config.s3Client();
        bucketName = config.bucketName();
        fetchMaxBytes = config.fetchMaxBytes();
        objectPrefix = config.objectPrefix();
    }

    @Override
    public void copyLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData
    ) throws RemoteStorageException {
        try {
            final ObjectMetadata segmentPutMetadata = new ObjectMetadata();
            segmentPutMetadata.setContentLength(Files.size(logSegmentData.logSegment()));
            final PutObjectRequest segmentPutRequest = new PutObjectRequest(
                bucketName,
                segmentKey(remoteLogSegmentMetadata),
                Files.newInputStream(logSegmentData.logSegment()),
                segmentPutMetadata);
            s3Client.putObject(segmentPutRequest);

            final ObjectMetadata offsetPutMetadata = new ObjectMetadata();
            offsetPutMetadata.setContentLength(Files.size(logSegmentData.offsetIndex()));
            final PutObjectRequest offsetPutRequest = new PutObjectRequest(
                bucketName,
                indexKey(remoteLogSegmentMetadata, IndexType.OFFSET),
                Files.newInputStream(logSegmentData.offsetIndex()),
                offsetPutMetadata);
            s3Client.putObject(offsetPutRequest);

            final ObjectMetadata timePutMetadata = new ObjectMetadata();
            timePutMetadata.setContentLength(Files.size(logSegmentData.timeIndex()));
            final PutObjectRequest timePutRequest = new PutObjectRequest(
                bucketName,
                indexKey(remoteLogSegmentMetadata, IndexType.TIMESTAMP),
                Files.newInputStream(logSegmentData.timeIndex()),
                timePutMetadata);
            s3Client.putObject(timePutRequest);

            if (logSegmentData.transactionIndex().isPresent()) {
                final ObjectMetadata txPutMetadata = new ObjectMetadata();
                txPutMetadata.setContentLength(Files.size(logSegmentData.transactionIndex().get()));
                final PutObjectRequest txPutRequest = new PutObjectRequest(
                    bucketName,
                    indexKey(remoteLogSegmentMetadata, IndexType.TRANSACTION),
                    Files.newInputStream(logSegmentData.transactionIndex().get()),
                    txPutMetadata);
                s3Client.putObject(txPutRequest);
            }

            final ObjectMetadata prodPutMetadata = new ObjectMetadata();
            prodPutMetadata.setContentLength(Files.size(logSegmentData.producerSnapshotIndex()));
            final PutObjectRequest prodPutRequest = new PutObjectRequest(
                bucketName,
                indexKey(remoteLogSegmentMetadata, IndexType.PRODUCER_SNAPSHOT),
                Files.newInputStream(logSegmentData.producerSnapshotIndex()),
                prodPutMetadata);
            s3Client.putObject(prodPutRequest);

            final ObjectMetadata leaderEpochPutMetadata = new ObjectMetadata();
            final int leaderEpochSize = logSegmentData.leaderEpochIndex().remaining();
            leaderEpochPutMetadata.setContentLength(leaderEpochSize);
            final PutObjectRequest leaderEpochPutRequest = new PutObjectRequest(
                bucketName,
                indexKey(remoteLogSegmentMetadata, IndexType.LEADER_EPOCH),
                new ByteBufferInputStream(logSegmentData.leaderEpochIndex()),
                leaderEpochPutMetadata);
            s3Client.putObject(leaderEpochPutRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final int offset
    ) throws RemoteStorageException {
        final String key = segmentKey(remoteLogSegmentMetadata);
        try {
            final GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setRange(offset);
            final S3Object s3Object = s3Client.getObject(request);
            LOG.info("Fetched segment: {} with length {}", key, s3Object.getObjectContent().available());
            return s3Object.getObjectContent();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final int startOffset,
        final int requestedEndOffset
    ) throws RemoteStorageException {
        final String key = segmentKey(remoteLogSegmentMetadata);
        final int endOffset = Math.min(requestedEndOffset, remoteLogSegmentMetadata.segmentSizeInBytes() - 1);
        LOG.info("Fetch segment: {} at offsets: [{}, {}]", key, startOffset, endOffset);
        try {
            final GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setRange(startOffset, endOffset);
            final S3Object s3Object = s3Client.getObject(request);
            LOG.info("Fetched segment: {} with length {}", key, s3Object.getObjectContent().available());
            return s3Object.getObjectContent();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream fetchIndex(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final IndexType indexType
    ) throws RemoteStorageException {
        final String key = indexKey(remoteLogSegmentMetadata, indexType);
        LOG.info("Fetch index ({}): {}", indexType.name(), key);
        final GetObjectRequest request = new GetObjectRequest(bucketName, key);
        try {
            final S3Object s3Object = s3Client.getObject(request);
            return s3Object.getObjectContent();
        } catch (final AmazonS3Exception e) {
            LOG.warn(e.getMessage());
            if (e.getMessage().startsWith("The specified key does not exist.")) {
                return null;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata
    ) throws RemoteStorageException {
        final DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName);
        final List<DeleteObjectsRequest.KeyVersion> keys = objectSet(remoteLogSegmentMetadata).values()
            .stream()
            .map(DeleteObjectsRequest.KeyVersion::new)
            .collect(Collectors.toList());
        request.setKeys(keys);
        s3Client.deleteObjects(request);
    }

    Map<String, String> objectSet(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final Map<String, String> set = new HashMap<>();
        set.put("SEGMENT", segmentKey(remoteLogSegmentMetadata));
        for (final IndexType indexType : IndexType.values()) {
            set.put(indexType.name(), indexKey(remoteLogSegmentMetadata, indexType));
        }
        return set;
    }

    private String segmentKey(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return key(remoteLogSegmentMetadata, SEGMENT_SUFFIX);
    }

    private String indexKey(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final IndexType indexType) {
        return key(remoteLogSegmentMetadata, indexSuffix(indexType));
    }

    private String key(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final String suffix) {
        return prefix(remoteLogSegmentMetadata)
            + remoteLogSegmentMetadata.remoteLogSegmentId().id().toString()
            + suffix;
    }

    private String prefix(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return objectPrefix + remoteLogSegmentMetadata.topicIdPartition().topicPartition().topic()
            + "-" + remoteLogSegmentMetadata.topicIdPartition().topicId().toString()
            + "/" + remoteLogSegmentMetadata.topicIdPartition().topicPartition().partition()
            + "/";
    }

    static String indexSuffix(final IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return OFFSET_INDEX_SUFFIX;
            case TIMESTAMP:
                return TIME_INDEX_SUFFIX;
            case TRANSACTION:
                return TX_INDEX_SUFFIX;
            case LEADER_EPOCH:
                return LEADER_EPOCH_SUFFIX;
            case PRODUCER_SNAPSHOT:
                return PRODUCER_SNAPSHOT_SUFFIX;
            default:
                throw new IllegalStateException("No index type found");
        }
    }

    @Override
    public void close() throws IOException {
    }
}
