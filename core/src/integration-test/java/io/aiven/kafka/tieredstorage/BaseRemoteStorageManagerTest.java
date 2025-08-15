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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseRemoteStorageManagerTest extends RsaKeyAwareTest {
    public static final byte[] LEADER_EPOCH_INDEX_BYTES = "leader epoch index".getBytes();
    static final int IV_SIZE = 12;
    static final int SEGMENT_SIZE = 10 * 1024 * 1024;
    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7));
    static final RemoteLogSegmentId REMOTE_SEGMENT_ID = new RemoteLogSegmentId(TOPIC_ID_PARTITION, SEGMENT_ID);
    static final long START_OFFSET = 23L;
    static final RemoteLogSegmentMetadata REMOTE_LOG_METADATA = new RemoteLogSegmentMetadata(
        REMOTE_SEGMENT_ID, START_OFFSET, 2000L,
        0, 0, 0, SEGMENT_SIZE, Map.of(0, 0L));
    static final String TARGET_LOG_FILE =
        "test/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.log";
    static final String TARGET_MANIFEST_FILE =
        "test/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest";

    static final SegmentCustomMetadataSerde CUSTOM_METADATA_SERDE = new SegmentCustomMetadataSerde();

    RsaEncryptionProvider rsaEncryptionProvider;
    AesEncryptionProvider aesEncryptionProvider;

    @TempDir
    Path tmpDir;
    Path targetDir;

    Path sourceDir;
    Path logFilePath;
    Path offsetIndexFilePath;
    Path timeIndexFilePath;
    Path producerSnapshotFilePath;
    Path txnIndexFilePath;

    @BeforeEach
    void init() throws IOException {
        rsaEncryptionProvider = new RsaEncryptionProvider(KEY_ENCRYPTION_KEY_ID, keyRing);
        aesEncryptionProvider = new AesEncryptionProvider();

        targetDir = Path.of(tmpDir.toString(), "target/");
        Files.createDirectories(targetDir);

        sourceDir = Path.of(tmpDir.toString(), "source");
        Files.createDirectories(sourceDir);

        logFilePath = Path.of(sourceDir.toString(), "00000000000000000023.log");
        createRandomFilledFile(logFilePath, SEGMENT_SIZE);

        offsetIndexFilePath = Path.of(sourceDir.toString(), "00000000000000000023.index");
        createRandomFilledFile(offsetIndexFilePath, 256 * 1024);

        timeIndexFilePath = Path.of(sourceDir.toString(), "00000000000000000023.timeindex");
        createRandomFilledFile(timeIndexFilePath, 256 * 1024);

        producerSnapshotFilePath = Path.of(sourceDir.toString(), "00000000000000000023.snapshot");
        createRandomFilledFile(producerSnapshotFilePath, 4 * 1024);

        txnIndexFilePath = Path.of(sourceDir.toString(), "00000000000000000023.txnindex");
        createRandomFilledFile(txnIndexFilePath, 128 * 1024);

        targetDir = Path.of(tmpDir.toString(), "target/");
        Files.createDirectories(targetDir);
    }

    private void createRandomFilledFile(final Path path, final int size) throws IOException {
        // Should be at least multiple of kilobyte.
        assertThat(size % 1024).isZero();

        int unit = 1024 * 1024;
        while (size % unit != 0) {
            unit /= 2;
        }

        final var random = new Random();
        final byte[] buf = new byte[unit];
        try (final var outputStream = Files.newOutputStream(path)) {
            for (int i = 0; i < size / unit; i++) {
                random.nextBytes(buf);
                outputStream.write(buf);
            }
        }
    }
}
