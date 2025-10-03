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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1Builder;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class KafkaRemoteStorageManagerTest extends BaseRemoteStorageManagerTest {
    @Mock
    Logger log;
    @Mock
    Time time;

    @ParameterizedTest
    @CsvSource({"none,true", "zstd,false"})
    void testRequiresCompression(final String compressionType, final boolean expectedResult)
        throws IOException {
        final Path logSegmentPath = targetDir.resolve("segment.log");
        final File logSegmentFile = logSegmentPath.toFile();
        try (final FileRecords records = FileRecords.open(logSegmentFile, false, 100000, true);
             final MemoryRecordsBuilder builder = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 Compression.of(compressionType).build(),
                 TimestampType.CREATE_TIME,
                 0)) {
            builder.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(builder.build());
        }

        // Configure the RSM.
        final int chunkSize = 1024 * 1024;
        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", Integer.toString(chunkSize),
            "storage.backend.class",
            "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "key.prefix", "test/",
            "storage.root", targetDir.toString(),
            "compression.enabled", "true",
            "compression.heuristic.enabled", "true",
            "fetch.chunk.cache.size", 10000,
            "fetch.chunk.cache.class", MemoryChunkCache.class.getCanonicalName(),
            "fetch.chunk.cache.retention.ms", 10000
        ));
        final KafkaRemoteStorageManager rsm = new KafkaRemoteStorageManager(log, time, config);

        final LogSegmentData logSegmentData = new LogSegmentData(
            logSegmentPath, offsetIndexFilePath, timeIndexFilePath, Optional.empty(),
            producerSnapshotFilePath, ByteBuffer.wrap(LEADER_EPOCH_INDEX_BYTES));

        final boolean requires = rsm.requiresCompression(logSegmentData);
        assertThat(requires).isEqualTo(expectedResult);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTransformingIndexes(final boolean encryption) {
        final var props = new HashMap<>(Map.of(
            "chunk.size", "10",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString(),
            "encryption.enabled", Boolean.toString(encryption)
        ));
        final DataKeyAndAAD maybeEncryptionKey;
        if (encryption) {
            props.put("encryption.key.pair.id", KEY_ENCRYPTION_KEY_ID);
            props.put("encryption.key.pairs", KEY_ENCRYPTION_KEY_ID);
            props.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".public.key.file", publicKeyPem.toString());
            props.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".private.key.file", privateKeyPem.toString());
            maybeEncryptionKey = aesEncryptionProvider.createDataKeyAndAAD();
        } else {
            maybeEncryptionKey = null;
        }
        final var config = new RemoteStorageManagerConfig(props);
        final KafkaRemoteStorageManager rsm = new KafkaRemoteStorageManager(log, time, config);

        final var segmentIndexBuilder = new SegmentIndexesV1Builder();
        final var bytes = "test".getBytes();
        final var is = rsm.transformIndex(
            RemoteStorageManager.IndexType.OFFSET,
            new ByteArrayInputStream(bytes),
            bytes.length,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        assertThat(is).isNotEmpty();
        assertThat(segmentIndexBuilder.indexes()).containsOnly(RemoteStorageManager.IndexType.OFFSET);


        // adding required indexes to test builder
        rsm.transformIndex(
            RemoteStorageManager.IndexType.TIMESTAMP,
            new ByteArrayInputStream(bytes),
            bytes.length,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        rsm.transformIndex(
            RemoteStorageManager.IndexType.LEADER_EPOCH,
            new ByteArrayInputStream(bytes),
            bytes.length,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        rsm.transformIndex(
            RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT,
            new ByteArrayInputStream(bytes),
            bytes.length,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        final var index = segmentIndexBuilder.build();
        assertThat(index.offset().size()).isGreaterThan(0);
        assertThat(index.timestamp().size()).isGreaterThan(0);
        assertThat(index.leaderEpoch().size()).isGreaterThan(0);
        assertThat(index.producerSnapshot().size()).isGreaterThan(0);
        assertThat(index.transaction()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTransformingEmptyIndexes(final boolean encryption) {
        final var props = new HashMap<>(Map.of(
            "chunk.size", "10",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString(),
            "encryption.enabled", Boolean.toString(encryption)
        ));
        final DataKeyAndAAD maybeEncryptionKey;
        if (encryption) {
            props.put("encryption.key.pair.id", KEY_ENCRYPTION_KEY_ID);
            props.put("encryption.key.pairs", KEY_ENCRYPTION_KEY_ID);
            props.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".public.key.file", publicKeyPem.toString());
            props.put("encryption.key.pairs." + KEY_ENCRYPTION_KEY_ID + ".private.key.file", privateKeyPem.toString());
            maybeEncryptionKey = aesEncryptionProvider.createDataKeyAndAAD();
        } else {
            maybeEncryptionKey = null;
        }
        final var config = new RemoteStorageManagerConfig(props);
        final KafkaRemoteStorageManager rsm = new KafkaRemoteStorageManager(log, time, config);

        final var segmentIndexBuilder = new SegmentIndexesV1Builder();
        final var is = rsm.transformIndex(
            RemoteStorageManager.IndexType.OFFSET,
            InputStream.nullInputStream(),
            0,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        assertThat(is).isEmpty();
        assertThat(segmentIndexBuilder.indexes()).containsOnly(RemoteStorageManager.IndexType.OFFSET);

        // adding required indexes to test builder
        rsm.transformIndex(
            RemoteStorageManager.IndexType.TIMESTAMP,
            InputStream.nullInputStream(),
            0,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        rsm.transformIndex(
            RemoteStorageManager.IndexType.LEADER_EPOCH,
            InputStream.nullInputStream(),
            0,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        rsm.transformIndex(
            RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT,
            InputStream.nullInputStream(),
            0,
            maybeEncryptionKey,
            segmentIndexBuilder
        );
        final var index = segmentIndexBuilder.build();
        assertThat(index.offset().size()).isEqualTo(0);
        assertThat(index.timestamp().size()).isEqualTo(0);
        assertThat(index.leaderEpoch().size()).isEqualTo(0);
        assertThat(index.producerSnapshot().size()).isEqualTo(0);
        assertThat(index.transaction()).isNull();
    }

    @Test
    void testGetIndexSizeWithInvalidPaths() {
        // non existing file
        assertThatThrownBy(() -> io.aiven.kafka.tieredstorage.KafkaRemoteStorageManager.indexSize(Path.of("non-exist")))
            .hasMessage("Error while getting index path size")
            .isInstanceOf(RemoteStorageException.class);
    }
}
