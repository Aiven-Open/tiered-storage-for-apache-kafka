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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import cloud.localstack.Localstack;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import kafka.log.LazyIndex;
import kafka.log.Log;
import kafka.log.LogSegment;
import kafka.log.OffsetIndex;
import kafka.log.TimeIndex;
import kafka.log.TransactionIndex;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {"s3"})
@Testcontainers
public class S3RemoteStorageManagerTest {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManagerTest.class);

    static final String TOPIC = "connect-log";
    static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition(TOPIC, 0));
    static final TopicIdPartition TP1 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition(TOPIC, 1));

    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    /* A consistent random number generator to make tests repeatable */
    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    private static Path publicKeyPem;
    private static Path privateKeyPem;

    File logDir;

    private List<LogSegment> createdSegments;

    private static AmazonS3 s3Client;

    private String bucket;
    private S3RemoteStorageManager remoteStorageManager;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeAll
    public static void setUpClass(@TempDir final Path tmpFolder)
            throws NoSuchAlgorithmException, NoSuchProviderException, IOException {
        s3Client = TestUtils.getClientS3();
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
        keyPairGenerator.initialize(2048, SecureRandom.getInstanceStrong());
        final KeyPair rsaKeyPair = keyPairGenerator.generateKeyPair();

        publicKeyPem = tmpFolder.resolve(Paths.get("test_public.pem"));
        privateKeyPem = tmpFolder.resolve(Paths.get("test_private.pem"));

        writePemFile(publicKeyPem, new X509EncodedKeySpec(rsaKeyPair.getPublic().getEncoded()));
        writePemFile(privateKeyPem, new PKCS8EncodedKeySpec(rsaKeyPair.getPrivate().getEncoded()));
    }

    @BeforeEach
    public void setUp() throws IOException {

        logDir = Files.createTempDirectory(null).toFile();
        createdSegments = new ArrayList<>();

        bucket = randomString(10).toLowerCase(Locale.ROOT);
        s3Client.createBucket(bucket);
        final AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                Localstack.INSTANCE.getEndpointS3(), "us-east-1");
        remoteStorageManager = new S3RemoteStorageManager(endpointConfiguration);
        remoteStorageManager.configure(basicProps(bucket));
    }

    protected static void writePemFile(final Path path, final EncodedKeySpec encodedKeySpec) throws IOException {
        try (final var pemWriter = new PemWriter(Files.newBufferedWriter(path))) {
            final var pemObject = new PemObject("SOME KEY", encodedKeySpec.getEncoded());
            pemWriter.writeObject(pemObject);
            pemWriter.flush();
        }
    }


    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++) {
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        }
        return b.toString();
    }


    @AfterEach
    public void tearDown() {
        remoteStorageManager.close();

        for (final LogSegment createdSegment : createdSegments) {
            createdSegment.close();
        }

        // Sometimes errors happen here on Windows machines.
        try {
            Utils.delete(logDir);
        } catch (final IOException e) {
            log.error("Error during tear down", e);
        }
    }


    @Test
    public void testCopyLogSegment() throws Exception {
        final LogSegment segment1 = createLogSegment(0);
        final LogSegment segment2 = createLogSegment(5);

        final Uuid uuid1 = Uuid.randomUuid();
        final RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(TP0, uuid1);

        final LogSegmentData lsd1 = createLogSegmentData(segment1);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId1, segment1), lsd1);

        final Uuid uuid2 = Uuid.randomUuid();
        final RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(TP1, uuid2);
        final LogSegmentData lsd2 = createLogSegmentData(segment2);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId2, segment2), lsd2);

        final List<String> keys = listS3Keys();
        final String baseLogFileName1 = segment1.log().file().getName();
        final String baseLogFileName2 = segment2.log().file().getName();
        assertThat(keys).containsExactlyInAnyOrder(
                s3Key(TP0, uuid1 + "-" + baseLogFileName1) + "_0",
                s3Key(TP0, uuid1 + "-" + baseLogFileName1) + "_1",
                s3Key(TP0, uuid1 + "-" + baseLogFileName1) + "_2",
                s3Key(TP0, uuid1 + "-" + baseLogFileName1) + "_3",
                s3Key(TP0, uuid1 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment1.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.OFFSET),
                s3Key(TP0, uuid1 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment1.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TIMESTAMP),
                s3Key(TP0, uuid1 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment1.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
                s3Key(TP0, uuid1 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment1.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TRANSACTION),
                s3Key(TP0, uuid1 + "-" + String.format("%020d", segment1.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.LEADER_EPOCH),
                s3Key(TP0, uuid1 + "-" + String.format("%020d", segment1.baseOffset()) + ".metadata.json"),
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_0",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_1",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_2",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_3",
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.OFFSET),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TIMESTAMP),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TRANSACTION),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.LEADER_EPOCH),
                s3Key(TP1, uuid2 + "-" + String.format("%020d", segment2.baseOffset()) + ".metadata.json")
        );
    }

    private LogSegmentData createLogSegmentData(final LogSegment segment) throws IOException {
        final File file = Log.producerSnapshotFile(logDir, segment.baseOffset());
        file.createNewFile();
        return new LogSegmentData(segment.log().file().toPath(), segment.offsetIndex().file().toPath(),
                segment.timeIndex().file().toPath(), Optional.of(segment.txnIndex().file().toPath()),
                file.toPath(),
                ByteBuffer.wrap(new byte[] {1, 2, 3}));
    }

    @Test
    public void testFetchLogSegmentDataComplete() throws Exception {
        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        final LogSegmentData lsd = createLogSegmentData(segment);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId, segment), lsd);

        final RemoteLogSegmentMetadata metadata =
                new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), -1, -1, -1, 1L,
                        segment.size(), Collections.singletonMap(1, 100L));
        try (final InputStream remoteInputStream = remoteStorageManager.fetchLogSegment(metadata, 0)) {
            assertStreamContentEqualToFile(segment.log().file(), 0, null, remoteInputStream);
        }
    }

    @Test
    public void testFetchLogSegmentDataPartially() throws Exception {
        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        final LogSegmentData lsd = createLogSegmentData(segment);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId, segment), lsd);

        final RemoteLogSegmentMetadata metadata =
                new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), -1, -1, -1, 1L,
                        segment.log().sizeInBytes(), Collections.singletonMap(1, 100L));

        final int skipBeginning = 23;
        final int startPosition = skipBeginning;
        final long skipEnd = 42L;
        final long endPosition = segment.log().file().length() - skipEnd;
        try (final InputStream remoteInputStream = remoteStorageManager.fetchLogSegment(metadata, startPosition,
                (int) endPosition)) {
            assertStreamContentEqualToFile(segment.log().file(), skipBeginning, skipEnd, remoteInputStream);
        }
    }

    @Test
    public void testFetchOffsetIndex() throws Exception {
        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        final LogSegmentData lsd = createLogSegmentData(segment);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId, segment), lsd);

        final RemoteLogSegmentMetadata metadata =
                new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), -1, -1, -1, 1L,
                        segment.log().sizeInBytes(),
                        Collections.singletonMap(1, 100L));

        try (final InputStream remoteInputStream = remoteStorageManager.fetchIndex(metadata,
                RemoteStorageManager.IndexType.OFFSET)) {
            assertStreamContentEqualToFile(segment.offsetIndex().file(), 0, null, remoteInputStream);
        }
    }

    @Test
    public void testFetchTimestampIndex() throws Exception {
        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        final LogSegmentData lsd = createLogSegmentData(segment);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId, segment), lsd);

        final RemoteLogSegmentMetadata metadata =
                new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), -1, -1, -1, 1L,
                        segment.log().sizeInBytes(), Collections.singletonMap(1, 100L));
        try (final InputStream remoteInputStream = remoteStorageManager.fetchIndex(metadata,
                RemoteStorageManager.IndexType.TIMESTAMP)) {
            assertStreamContentEqualToFile(segment.timeIndex().file(), 0, null, remoteInputStream);
        }
    }

    @Test
    public void testDeleteLogSegment() throws Exception {
        final LogSegment segment1 = createLogSegment(0);
        final LogSegment segment2 = createLogSegment(5);

        final RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        final LogSegmentData lsd1 = createLogSegmentData(segment1);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId1, segment1), lsd1);

        final Uuid uuid2 = Uuid.randomUuid();
        final RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(TP1, uuid2);
        final LogSegmentData lsd2 = createLogSegmentData(segment2);
        remoteStorageManager.copyLogSegmentData(createLogSegmentMetadata(segmentId2, segment2), lsd2);

        final RemoteLogSegmentMetadata metadata1 =
                new RemoteLogSegmentMetadata(segmentId1, 0, -1, -1, -1, 1L, segment1.log().sizeInBytes(),
                        Collections.singletonMap(1, 100L));
        remoteStorageManager.deleteLogSegmentData(metadata1);

        final List<String> keys = listS3Keys();
        final String baseLogFileName2 = segment2.log().file().getName();
        assertThat(keys).containsOnly(
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_0",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_1",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_2",
                s3Key(TP1, uuid2 + "-" + baseLogFileName2) + "_3",
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.OFFSET),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TIMESTAMP),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.TRANSACTION),
                s3Key(TP1, uuid2 + "-" + S3StorageUtils.filenamePrefixFromOffset(segment2.baseOffset()) + "."
                        + RemoteStorageManager.IndexType.LEADER_EPOCH),
                s3Key(TP1, uuid2 + "-" + String.format("%020d", segment2.baseOffset()) + ".metadata.json")
        );
    }

    private static void assertStreamContentEqualToFile(final File expectedFile,
                                                       final int skipBeginning, final Long skipEnd,
                                                       final InputStream actual) throws IOException {
        int length = (int) expectedFile.length();
        if (skipEnd != null) {
            length -= skipEnd;
        }

        try (final InputStream fileInputStream = new FileInputStream(expectedFile)) {
            final long skipped = fileInputStream.skip(skipBeginning);
            assertThat(skipped)
                    .withFailMessage("Failed tp skip the beginning of the excepted file <%s>", expectedFile.getName())
                    .isEqualTo(skipBeginning);
            final long skip = actual.skip(skipBeginning);
            assertThat(skip)
                    .withFailMessage("Failed tp skip the beginning of the actual stream")
                    .isEqualTo(skipBeginning);
            length -= skipBeginning;
            assertThat(actual.readAllBytes()).isEqualTo(fileInputStream.readNBytes(length));
        }
    }

    private static Map<String, String> basicProps(final String bucket) {
        final Map<String, String> props = new HashMap<>();
        props.put(S3RemoteStorageManagerConfig.S3_BUCKET_NAME_CONFIG, bucket);
        props.put(S3RemoteStorageManagerConfig.S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                AnonymousCredentialsProvider.class.getName());
        props.put(S3RemoteStorageManagerConfig.PUBLIC_KEY, publicKeyPem.toString());
        props.put(S3RemoteStorageManagerConfig.PRIVATE_KEY, privateKeyPem.toString());
        props.put(S3RemoteStorageManagerConfig.IO_BUFFER_SIZE, "8192");
        props.put(S3RemoteStorageManagerConfig.S3_STORAGE_UPLOAD_PART_SIZE, String.valueOf(1 << 19));
        props.put(S3RemoteStorageManagerConfig.MULTIPART_UPLOAD_PART_SIZE, "8192");
        return props;
    }

    private static RemoteLogSegmentMetadata createLogSegmentMetadata(final RemoteLogSegmentId remoteLogSegmentId,
                                                                     final LogSegment logSegment) {
        return new RemoteLogSegmentMetadata(remoteLogSegmentId, logSegment.baseOffset(),
                logSegment.readNextOffset() - 1, logSegment.largestTimestamp(),
                0, logSegment.time().milliseconds(), logSegment.size(), Collections.singletonMap(1, 100L));
    }

    private LogSegment createLogSegment(final long offset) throws IOException {
        final LogSegment segment = createSegment(offset, logDir, 2048, Time.SYSTEM);
        createdSegments.add(segment);

        final int batchSize = 100;
        for (long i = offset; i < batchSize * 20; i += batchSize) {
            appendRecordBatch(segment, i, 1024, batchSize);
        }

        segment.onBecomeInactiveSegment();
        return segment;
    }

    private static void appendRecordBatch(final LogSegment segment, final long offset, final int recordSize,
                                          final int numRecords) {
        final SimpleRecord[] records = new SimpleRecord[numRecords];
        long timestamp = 0;
        for (int i = 0; i < numRecords; i++) {
            timestamp = (offset + i) * 10000000;
            records[i] = new SimpleRecord(timestamp, new byte[recordSize]);
            records[i].value().putLong(0, offset + i);
            records[i].value().rewind();
        }
        final long lastOffset = offset + numRecords - 1;
        final MemoryRecords memoryRecords = MemoryRecords.withRecords(
                RecordBatch.CURRENT_MAGIC_VALUE, offset, CompressionType.NONE, TimestampType.CREATE_TIME, records);
        segment.append(lastOffset, timestamp, offset, memoryRecords);
    }

    private List<String> listS3Keys() {
        final List<S3ObjectSummary> objects = s3Client.listObjects(bucket).getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private static String s3Key(final TopicIdPartition topicIdPartition, final String name) {
        return topicIdPartition.topicPartition().topic() + "-" + topicIdPartition.topicId() + "/"
                + topicIdPartition.topicPartition().partition() + "/" + name;
    }

    static class AnonymousCredentialsProvider implements AWSCredentialsProvider {
        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("foo", "bar");
        }

        @Override
        public void refresh() {

        }
    }

    /**
     * Create a segment with the given base offset
     */
    static LogSegment createSegment(final Long offset,
                                    final File logDir,
                                    final int indexIntervalBytes,
                                    final Time time) throws IOException {
        final FileRecords ms = FileRecords.open(Log.logFile(logDir, offset, ""));
        final LazyIndex<OffsetIndex> idx =
                LazyIndex.forOffset(Log.offsetIndexFile(logDir, offset, ""), offset, 1000, true);
        final LazyIndex<TimeIndex> timeIdx =
                LazyIndex.forTime(Log.timeIndexFile(logDir, offset, ""), offset, 1500, true);
        final File file = Log.transactionIndexFile(logDir, offset, "");
        file.createNewFile();
        final TransactionIndex txnIndex = new TransactionIndex(offset, file);

        return new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time);
    }

}
