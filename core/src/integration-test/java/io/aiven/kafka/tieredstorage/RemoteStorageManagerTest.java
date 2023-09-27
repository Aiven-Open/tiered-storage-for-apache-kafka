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

package io.aiven.kafka.tieredstorage;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCache;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.security.EncryptedDataKey;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.transform.KeyNotFoundRuntimeException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteStorageManagerTest extends RsaKeyAwareTest {
    RemoteStorageManager rsm;

    RsaEncryptionProvider rsaEncryptionProvider;
    AesEncryptionProvider aesEncryptionProvider;

    @TempDir
    Path tmpDir;
    Path sourceDir;
    Path targetDir;

    Path logFilePath;
    Path offsetIndexFilePath;
    Path timeIndexFilePath;
    Path producerSnapshotFilePath;
    Path txnIndexFilePath;

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

    private static List<Arguments> provideEndToEnd() {
        final List<Arguments> result = new ArrayList<>();
        final var cacheNames =
            List.of(InMemoryChunkCache.class.getCanonicalName(), DiskBasedChunkCache.class.getCanonicalName());
        for (final String cacheClass : cacheNames) {
            for (final int chunkSize : List.of(1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, Integer.MAX_VALUE / 2)) {
                for (final boolean compression : List.of(true, false)) {
                    for (final boolean encryption : List.of(true, false)) {
                        for (final boolean hasTxnIndex : List.of(true, false)) {
                            result.add(Arguments.of(cacheClass, chunkSize, compression, encryption, hasTxnIndex));
                        }
                    }
                }
            }
        }
        return result;
    }

    @BeforeEach
    void init() throws IOException {
        rsm = new RemoteStorageManager();

        rsaEncryptionProvider = new RsaEncryptionProvider(KEY_ENCRYPTION_KEY_ID, keyRing);
        aesEncryptionProvider = new AesEncryptionProvider();

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

    @ParameterizedTest(name = "{argumentsWithNames}")
    @MethodSource("provideEndToEnd")
    void endToEnd(
        final String cacheClass,
        final int chunkSize,
        final boolean compression,
        final boolean encryption,
        final boolean hasTxnIndex) throws RemoteStorageException, IOException {
        // Configure the RSM.
        final var cacheDir = tmpDir.resolve("cache");
        Files.createDirectories(cacheDir);
        final Map<String, String> config = new HashMap<>(Map.of(
            "chunk.size", Integer.toString(chunkSize),
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "key.prefix", "test/",
            "storage.root", targetDir.toString(),
            "compression.enabled", Boolean.toString(compression),
            "encryption.enabled", Boolean.toString(encryption),
            "chunk.cache.class", cacheClass,
            "chunk.cache.path", cacheDir.toString(),
            "chunk.cache.size", Integer.toString(100 * 1024 * 1024),
            "custom.metadata.fields.include", "REMOTE_SIZE,OBJECT_PREFIX,OBJECT_KEY"
        ));
        if (encryption) {
            config.put("encryption.key.pair.id", KEY_ENCRYPTION_KEY_ID);
            config.put("encryption.key.pairs", KEY_ENCRYPTION_KEY_ID);
            config.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".public.key.file", publicKeyPem.toString());
            config.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".private.key.file", privateKeyPem.toString());
        }
        rsm.configure(config);

        // Copy the segment.
        final Optional<Path> txnIndexPath = hasTxnIndex
            ? Optional.of(txnIndexFilePath)
            : Optional.empty();
        final LogSegmentData logSegmentData = new LogSegmentData(
            logFilePath, offsetIndexFilePath, timeIndexFilePath, txnIndexPath,
            producerSnapshotFilePath, ByteBuffer.wrap(LEADER_EPOCH_INDEX_BYTES));
        final Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadata =
            rsm.copyLogSegmentData(REMOTE_LOG_METADATA, logSegmentData);

        checkCustomMetadata(customMetadata);
        checkFilesInTargetDirectory(hasTxnIndex);
        checkManifest(chunkSize, compression, encryption);
        if (encryption) {
            checkEncryption(compression);
        }
        checkIndexContents(hasTxnIndex);
        checkFetching(chunkSize);
        checkDeletion();
    }

    private void checkCustomMetadata(final Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadata) {
        assertThat(customMetadata).isPresent();
        final var fields = CUSTOM_METADATA_SERDE.deserialize(customMetadata.get().value());
        assertThat(fields).hasSize(3);
        assertThat(fields.get(SegmentCustomMetadataField.REMOTE_SIZE.index()))
            .asInstanceOf(InstanceOfAssertFactories.LONG)
            .isGreaterThan(0);
        assertThat(fields.get(SegmentCustomMetadataField.OBJECT_KEY.index()))
            .asInstanceOf(InstanceOfAssertFactories.STRING)
            .isNotEmpty();
        assertThat(fields.get(SegmentCustomMetadataField.OBJECT_PREFIX.index()))
            .asInstanceOf(InstanceOfAssertFactories.STRING)
            .isNotEmpty();
    }

    private void checkFilesInTargetDirectory(final boolean hasTxnIndex) {
        final List<String> expectedFiles = new ArrayList<>(List.of(
            TARGET_LOG_FILE,
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.timeindex",
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.snapshot",
            "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint",
            TARGET_MANIFEST_FILE
        ));
        if (hasTxnIndex) {
            expectedFiles.add("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        }
        expectedFiles.forEach(s ->
            assertThat(targetDir).isDirectoryRecursivelyContaining(path -> path.toString().endsWith(s)));
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

        if (compression) {
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
            org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(offsetIndexFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(timeIndexFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(Files.readAllBytes(producerSnapshotFilePath));
        }
        try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
            org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH)) {
            assertThat(inputStream.readAllBytes())
                .isEqualTo(LEADER_EPOCH_INDEX_BYTES);
        }
        if (hasTxnIndex) {
            try (final var inputStream = rsm.fetchIndex(REMOTE_LOG_METADATA,
                org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION)) {
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
        objectMapper.registerModule(KafkaTypeSerdeModule.create());
        final JsonNode manifest = objectMapper.readTree(new File(targetDir.toString(), TARGET_MANIFEST_FILE));

        final String dataKeyText = manifest.get("encryption").get("dataKey").asText();
        final String[] dataKeyTextParts = dataKeyText.split(":");
        assertThat(dataKeyTextParts).hasSize(2);
        assertThat(dataKeyTextParts[0]).isEqualTo(KEY_ENCRYPTION_KEY_ID);
        final byte[] encryptedDataKey = Base64.getDecoder().decode(dataKeyTextParts[1]);
        final byte[] dataKey = rsaEncryptionProvider.decryptDataKey(
            new EncryptedDataKey(KEY_ENCRYPTION_KEY_ID, encryptedDataKey));
        final byte[] aad = manifest.get("encryption").get("aad").binaryValue();
        objectMapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));

        final ChunkIndex chunkIndex = objectMapper.treeToValue(manifest.get("chunkIndex"), ChunkIndex.class);

        try (final InputStream originalInputStream = Files.newInputStream(logFilePath);
             final InputStream transformedInputStream = Files.newInputStream(targetDir.resolve(TARGET_LOG_FILE))) {
            for (final Chunk chunk : chunkIndex.chunks()) {
                final byte[] originalChunk = originalInputStream.readNBytes(chunk.originalSize);
                final byte[] transformedChunk = transformedInputStream.readNBytes(chunk.transformedSize);
                byte[] detransformedChunk;
                try {
                    final DataKeyAndAAD dataKeyAndAAD = aesEncryptionProvider.createDataKeyAndAAD();
                    final Cipher cipher = aesEncryptionProvider.encryptionCipher(dataKeyAndAAD);
                    final byte[] iv = cipher.getIV();
                    final int ivSize = iv.length;
                    final SecretKeySpec secretKeySpec = new SecretKeySpec(dataKey, "AES");
                    cipher.init(Cipher.DECRYPT_MODE, secretKeySpec,
                        new GCMParameterSpec(AesEncryptionProvider.GCM_TAG_LENGTH, transformedChunk, 0, ivSize),
                        SecureRandom.getInstanceStrong());
                    cipher.updateAAD(aad);

                    detransformedChunk = cipher.doFinal(transformedChunk, IV_SIZE, transformedChunk.length - IV_SIZE);
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

        final int actualChunkSize = (int) Math.min(chunkSize, Files.size(logFilePath) - 2);
        // TODO more combinations?
        for (final int readSize : List.of(
            1,
            13,
            actualChunkSize / 2,
            actualChunkSize,
            actualChunkSize + 1,
            actualChunkSize * 2
        )) {
            final byte[] expectedBytes = new byte[readSize];
            for (final int offset : List.of(
                0,
                1,
                23,
                actualChunkSize / 2,
                actualChunkSize - 1,
                actualChunkSize,
                actualChunkSize + 1
            )) {
                final int read;
                try (RandomAccessFile r = new RandomAccessFile(logFilePath.toString(), "r")) {
                    r.seek(offset);
                    read = r.read(expectedBytes, 0, readSize);
                }
                final int inclusiveEndPosition = offset + readSize - 1;
                try (InputStream actual = rsm.fetchLogSegment(REMOTE_LOG_METADATA, offset, inclusiveEndPosition)) {
                    assertThat(actual.readAllBytes())
                        .isEqualTo(Arrays.copyOfRange(expectedBytes, 0, read));
                }
            }
        }
        // TODO test beyond size
    }

    private void checkDeletion() throws RemoteStorageException {
        rsm.deleteLogSegmentData(REMOTE_LOG_METADATA);
        assertThat(targetDir).isEmptyDirectory();
    }

    @ParameterizedTest
    @CsvSource({"NONE,true", "ZSTD,false"})
    void testRequiresCompression(final CompressionType compressionType, final boolean expectedResult)
        throws IOException {
        final Path logSegmentPath = targetDir.resolve("segment.log");
        final File logSegmentFile = logSegmentPath.toFile();
        try (final FileRecords records = FileRecords.open(logSegmentFile, false, 100000, true);
             final MemoryRecordsBuilder builder = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 compressionType,
                 TimestampType.CREATE_TIME,
                 0)) {
            builder.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(builder.build());
        }

        // Configure the RSM.
        final int chunkSize = 1024 * 1024;
        final Map<String, ?> config = new HashMap<>() {{
                put("chunk.size", Integer.toString(chunkSize));
                put("storage.backend.class",
                    "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage");
                put("key.prefix", "test/");
                put("storage.root", targetDir.toString());
                put("compression.enabled", "true");
                put("compression.heuristic.enabled", "true");
                put("chunk.cache.size", 10000);
                put("chunk.cache.class", InMemoryChunkCache.class.getCanonicalName());
                put("chunk.cache.retention.ms", 10000);
            }};

        rsm.configure(config);

        // When
        final LogSegmentData logSegmentData = new LogSegmentData(
            logSegmentPath, offsetIndexFilePath, timeIndexFilePath, Optional.empty(),
            producerSnapshotFilePath, ByteBuffer.wrap(LEADER_EPOCH_INDEX_BYTES));

        final boolean requires = rsm.requiresCompression(logSegmentData);
        assertThat(requires).isEqualTo(expectedResult);
    }

    @Test
    void testFetchingSegmentFileNonExistent() throws IOException {
        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);

        final ObjectKey objectKey = new ObjectKey("");

        // Ensure the manifest exists.
        writeManifest(objectKey);

        // Make sure the exception is connected to the log file.
        final String expectedMessage =
            "Key " + objectKey.key(REMOTE_LOG_METADATA, ObjectKey.Suffix.LOG) + " does not exists in storage";

        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundRuntimeException.class)
            .hasStackTraceContaining(expectedMessage);
        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0, 100))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundRuntimeException.class)
            .hasStackTraceContaining(expectedMessage);
    }

    @Test
    void testFetchingSegmentManifestNotFound() {
        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);

        // Make sure the exception is connected to the manifest file.
        final ObjectKey objectKey = new ObjectKey("");
        final String expectedMessage =
            "Key " + objectKey.key(REMOTE_LOG_METADATA, ObjectKey.Suffix.MANIFEST) + " does not exists in storage";

        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundException.class)
            .hasStackTraceContaining(expectedMessage);
        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0, 100))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundException.class)
            .hasStackTraceContaining(expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(IndexType.class)
    void testFetchingIndexNonExistent(final IndexType indexType) throws IOException {
        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);

        final ObjectKey objectKey = new ObjectKey("");

        // Ensure the manifest exists.
        writeManifest(objectKey);

        // Make sure the exception is connected to the index file.
        final String expectedMessage =
            "Key " + objectKey.key(REMOTE_LOG_METADATA, ObjectKey.Suffix.fromIndexType(indexType))
                + " does not exists in storage";

        assertThatThrownBy(() -> rsm.fetchIndex(REMOTE_LOG_METADATA, indexType))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundException.class)
            .hasStackTraceContaining(expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(IndexType.class)
    void testFetchingIndexManifestNotFound(final IndexType indexType) throws StorageBackendException, IOException {
        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);

        // Make sure the exception is connected to the manifest file.
        final ObjectKey objectKey = new ObjectKey("");
        final String expectedMessage =
            "Key " + objectKey.key(REMOTE_LOG_METADATA, ObjectKey.Suffix.MANIFEST) + " does not exists in storage";

        assertThatThrownBy(() -> rsm.fetchIndex(REMOTE_LOG_METADATA, indexType))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCauseInstanceOf(KeyNotFoundException.class)
            .hasStackTraceContaining(expectedMessage);
    }

    private void writeManifest(final ObjectKey objectKey) throws IOException {
        // Ensure the manifest exists.
        final String manifest =
            "{\"version\":\"1\","
                + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
                + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
                + "\"compression\":false}";
        final Path manifestPath = targetDir.resolve(objectKey.key(REMOTE_LOG_METADATA, ObjectKey.Suffix.MANIFEST));
        Files.createDirectories(manifestPath.getParent());
        Files.writeString(manifestPath, manifest);
    }
}
