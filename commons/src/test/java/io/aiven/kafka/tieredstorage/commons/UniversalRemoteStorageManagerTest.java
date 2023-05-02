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

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;

import com.github.luben.zstd.Zstd;
import org.apache.commons.io.input.BoundedInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class UniversalRemoteStorageManagerTest extends RsaKeyAwareTest {
    RemoteStorageManager rsm;

    RsaEncryptionProvider rsaEncryptionProvider;

    @TempDir
    Path tmpDir;
    Path sourceDir;
    Path targetDir;

    Path logFilePath;
    Path offsetIndexFilePath;
    Path timeIndexFilePath;
    Path producerSnapshotFilePath;
    Path txnIndexFilePath;

    static final int SEGMENT_SIZE = 10 * 1024 * 1024;
    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7));
    static final RemoteLogSegmentId REMOTE_SEGMENT_ID = new RemoteLogSegmentId(TOPIC_ID_PARTITION, SEGMENT_ID);
    static final long START_OFFSET = 23L;
    static final RemoteLogSegmentMetadata REMOTE_LOG_METADATA = new RemoteLogSegmentMetadata(
        REMOTE_SEGMENT_ID, START_OFFSET, 2000L,
        0, 0, 0, SEGMENT_SIZE, Map.of(0, 0L));

    static final ByteBuffer LEADER_EPOCH_INDEX = ByteBuffer.wrap("leader epoch index".getBytes());

    static final String TARGET_LOG_FILE =
        "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.log";
    static final String TARGET_MANIFEST_FILE =
        "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest";

    @BeforeEach
    void init() throws IOException {
        rsm = new UniversalRemoteStorageManager();
        rsaEncryptionProvider = RsaEncryptionProvider.of(publicKeyPem, privateKeyPem);

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

        targetDir = Path.of(tmpDir.toString(), "target");
        Files.createDirectories(targetDir);
    }

    private void createRandomFilledFile(final Path path, final int size) throws IOException {
        // Should be at least multiple of kilobyte.
        assertThat(size % 1024).isEqualTo(0);

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

    @ParameterizedTest
    @MethodSource("provideEndToEnd")
    void endToEnd(final int chunkSize,
                  final boolean compression,
                  final boolean encryption,
                  final boolean hasTxnIndex) throws RemoteStorageException, IOException {
        // Configure the RSM.
        final Map<String, String> config = new HashMap<>(Map.of(
            "chunk.size", Integer.toString(chunkSize),
            "object.storage.factory",
            "io.aiven.kafka.tieredstorage.commons.storage.filesystem.FileSystemStorageFactory",
            "object.storage.root", targetDir.toString(),
            "compression.enabled", Boolean.toString(compression),
            "encryption.enabled", Boolean.toString(encryption)
        ));
        if (encryption) {
            config.put("encryption.public.key.file", publicKeyPem.toString());
            config.put("encryption.private.key.file", privateKeyPem.toString());
        }
        rsm.configure(config);

        // Copy the segment.
        final Optional<Path> txnIndexPath = hasTxnIndex
            ? Optional.of(txnIndexFilePath)
            : Optional.empty();
        final LogSegmentData logSegmentData = new LogSegmentData(
            logFilePath, offsetIndexFilePath, timeIndexFilePath, txnIndexPath,
            producerSnapshotFilePath, LEADER_EPOCH_INDEX);
        rsm.copyLogSegmentData(REMOTE_LOG_METADATA, logSegmentData);

        checkFilesInTargetDirectory(hasTxnIndex);
        checkManifest(chunkSize, compression, encryption);
        if (encryption) {
            checkEncryption(compression);
        }
        checkIndexContents(hasTxnIndex);
        checkFetching(chunkSize);
        checkDeletion();
    }

    private static List<Arguments> provideEndToEnd() {
        final List<Arguments> result = new ArrayList<>();
        for (final int chunkSize : List.of(1024 - 1, 4 * 1024, 1024 * 1024 * 1024 - 1, Integer.MAX_VALUE)) {
            for (final boolean compression : List.of(true, false)) {
                for (final boolean encryption : List.of(true, false)) {
                    for (final boolean hasTxnIndex : List.of(true, false)) {
                        result.add(Arguments.of(chunkSize, compression, encryption, hasTxnIndex));
                    }
                }
            }
        }
        return result;
    }

    private void checkFilesInTargetDirectory(final boolean hasTxnIndex) throws IOException {
        final List<String> expectedFiles = new ArrayList<>(List.of(
            TARGET_LOG_FILE,
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index",
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex",
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot",
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint",
            TARGET_MANIFEST_FILE
        ));
        if (hasTxnIndex) {
            expectedFiles.add("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        }
        assertThat(Files.list(targetDir).map(Path::toString))
            .containsExactlyInAnyOrderElementsOf(expectedFiles);
    }

    private void checkManifest(final int chunkSize,
                               final boolean compression,
                               final boolean encryption) throws IOException {
        // Check the manifest.
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode manifest = objectMapper.readTree(new File(targetDir.toString(), TARGET_MANIFEST_FILE));
        final JsonNode chunkIndex = manifest.get("chunkIndex");

        assertThat(chunkIndex.get("originalChunkSize").asInt()).isEqualTo(chunkSize);
        assertThat(chunkIndex.get("originalFileSize").asInt()).isEqualTo(SEGMENT_SIZE);

        if (compression || encryption) {
            assertThat(chunkIndex.get("type").asText()).isEqualTo("variable");
            assertThat(chunkIndex.get("transformedChunks").asText()).isNotNull();
        } else {
            assertThat(chunkIndex.get("type").asText()).isEqualTo("fixed");
            assertThat(chunkIndex.get("transformedChunkSize").asInt()).isNotNull();
            assertThat(chunkIndex.get("finalTransformedChunkSize").asInt()).isNotNull();
        }

        assertThat(manifest.get("compression").asBoolean()).isEqualTo(compression);
        final JsonNode encryptionNode = manifest.get("encryption");
        if (encryption) {
            assertThat(encryptionNode).isNotNull();
        } else {
            assertThat(encryptionNode).isNull();
        }
    }

    private void checkIndexContents(final boolean hasTxnIndex) throws IOException, RemoteStorageException {
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            RemoteStorageManager.IndexType.OFFSET)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(offsetIndexFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            RemoteStorageManager.IndexType.TIMESTAMP)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(timeIndexFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(producerSnapshotFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            RemoteStorageManager.IndexType.LEADER_EPOCH)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(LEADER_EPOCH_INDEX.array());
        }
        if (hasTxnIndex) {
            try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
                RemoteStorageManager.IndexType.TRANSACTION)) {
                assertThat(inputStream.readAllBytes())
                    .isEqualTo(Files.readAllBytes(txnIndexFilePath));
            }
        }
    }

    private void checkEncryption(final boolean compression) throws IOException {
        // Try to decrypt the key and decrypt the chunks.
        // This checks:
        // 1. The key is encrypted.
        // 2. The key is really used for encryption.
        // 3. The AAD is used.

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode manifest = objectMapper.readTree(new File(targetDir.toString(), TARGET_MANIFEST_FILE));

        final byte[] encryptedDataKey = manifest.get("encryption").get("dataKey").binaryValue();
        final byte[] dataKey = rsaEncryptionProvider.decryptDataKey(encryptedDataKey);
        final byte[] aad = manifest.get("encryption").get("aad").binaryValue();

        final ChunkIndex chunkIndex = objectMapper.treeToValue(manifest.get("chunkIndex"), ChunkIndex.class);

        try (final InputStream originalInputStream = Files.newInputStream(Path.of(TARGET_LOG_FILE));
             final InputStream transformedInputStream = Files.newInputStream(logFilePath)) {
            for (final Chunk chunk : chunkIndex.chunks()) {
                final byte[] originalChunk = originalInputStream.readNBytes(chunk.originalSize);
                final byte[] transformedChunk = transformedInputStream.readNBytes(chunk.transformedSize);
                byte[] detransformedChunk;
                try {
                    final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", "BC");
                    final int ivSize = cipher.getIV().length;
                    final SecretKeySpec secretKeySpec = new SecretKeySpec(dataKey, "AES");
                    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec,
                        new IvParameterSpec(transformedChunk, 0, ivSize),
                        SecureRandom.getInstanceStrong());
                    cipher.updateAAD(aad);

                    detransformedChunk = cipher.doFinal(transformedChunk, ivSize, transformedChunk.length - ivSize);
                } catch (final GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }

                if (compression) {
                    final byte[] decompressChunk = new byte[chunk.originalSize];
                    Zstd.decompress(decompressChunk, detransformedChunk);
                    detransformedChunk = decompressChunk;
                }

                assertThat(detransformedChunk).isEqualTo(originalChunk);
            }
        }
    }

    private void checkFetching(final int chunkSize) throws RemoteStorageException, IOException {
        // Full fetch.
        try (final InputStream expectedInputStream = Files.newInputStream(logFilePath)) {
            assertThat(rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0))
                .hasSameContentAs(expectedInputStream);
        }

        // TODO more combinations?
        for (final int readSize : List.of(1, 13, chunkSize / 2, chunkSize, chunkSize + 1, chunkSize * 2)) {
            for (final int offset : List.of(0, 1, 23, chunkSize / 2, chunkSize - 1, chunkSize, chunkSize + 1,
                SEGMENT_SIZE - readSize - 10, SEGMENT_SIZE - readSize)) {
                try (final InputStream expectedInputStream = new BoundedInputStream(
                    Files.newInputStream(logFilePath), offset + readSize)) {
                    expectedInputStream.skip(offset);

                    assertThat(rsm.fetchLogSegment(REMOTE_LOG_METADATA, offset, offset + readSize))
                        .hasSameContentAs(expectedInputStream);
                }
            }
        }
        // TODO test beyond size
    }

    private void checkDeletion() throws RemoteStorageException {
        rsm.deleteLogSegmentData(REMOTE_LOG_METADATA);
        assertThat(targetDir).isEmptyDirectory();
    }
}
