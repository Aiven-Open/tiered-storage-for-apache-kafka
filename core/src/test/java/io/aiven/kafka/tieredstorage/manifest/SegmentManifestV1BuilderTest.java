/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.manifest;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.Map;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentManifestV1BuilderTest {
    static final FixedSizeChunkIndex CHUNK_INDEX =
        new FixedSizeChunkIndex(100, 1000, 110, 110);
    static final SecretKey DATA_KEY = new SecretKeySpec(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "AES");
    static final byte[] AAD = {10, 11, 12, 13};
    static final SegmentIndexesV1 SEGMENT_INDEXES = new SegmentIndexesV1Builder()
        .add(RemoteStorageManager.IndexType.OFFSET, 1)
        .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
        .add(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, 1)
        .add(RemoteStorageManager.IndexType.LEADER_EPOCH, 1)
        .add(RemoteStorageManager.IndexType.TRANSACTION, 1)
        .build();
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA = new RemoteLogSegmentMetadata(
        new RemoteLogSegmentId(
            new TopicIdPartition(Uuid.fromString("lZ6vvmajTWKDBUTV6SQAtQ"), 42, "topic1"),
            Uuid.fromString("adh9f8BMS4anaUnD8KWfWg")
        ),
        0,
        1000L,
        1000000000L,
        2,
        2000000000L,
        100500,
        Map.of(0, 100L, 1, 200L, 2, 300L)
    );

    @Test
    void minimal() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES).build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isFalse();
        assertThat(manifest.encryption()).isEmpty();
        assertThat(manifest.remoteLogSegmentMetadata()).isNull();
    }

    @Test
    void withRlsm() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES)
            .withRlsm(REMOTE_LOG_SEGMENT_METADATA)
            .build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isFalse();
        assertThat(manifest.encryption()).isEmpty();
        assertThat(manifest.remoteLogSegmentMetadata()).isEqualTo(REMOTE_LOG_SEGMENT_METADATA);
    }

    @Test
    void withCompressionEnabled() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES)
            .withCompressionEnabled(true)
            .build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isTrue();
        assertThat(manifest.encryption()).isEmpty();
        assertThat(manifest.remoteLogSegmentMetadata()).isNull();
    }

    @Test
    void withCompressionDisabled() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES)
            .withCompressionEnabled(false)
            .build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isFalse();
        assertThat(manifest.encryption()).isEmpty();
        assertThat(manifest.remoteLogSegmentMetadata()).isNull();
    }

    @Test
    void withEncryptionKey() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES)
            .withEncryptionKey(new DataKeyAndAAD(DATA_KEY, AAD))
            .build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isFalse();
        assertThat(manifest.encryption()).isPresent();
        manifest.encryption().ifPresent(segmentEncryptionMetadata -> {
            assertThat(segmentEncryptionMetadata.dataKey()).isEqualTo(DATA_KEY);
            assertThat(segmentEncryptionMetadata.aad()).isEqualTo(AAD);
        });
        assertThat(manifest.remoteLogSegmentMetadata()).isNull();
    }

    @Test
    void full() {
        final var manifest = SegmentManifestV1.newBuilder(CHUNK_INDEX, SEGMENT_INDEXES)
            .withCompressionEnabled(true)
            .withEncryptionKey(new DataKeyAndAAD(DATA_KEY, AAD))
            .withRlsm(REMOTE_LOG_SEGMENT_METADATA)
            .build();
        assertThat(manifest.chunkIndex()).isEqualTo(CHUNK_INDEX);
        assertThat(manifest.segmentIndexes()).isEqualTo(SEGMENT_INDEXES);
        assertThat(manifest.compression()).isTrue();
        assertThat(manifest.encryption()).isPresent();
        manifest.encryption().ifPresent(segmentEncryptionMetadata -> {
            assertThat(segmentEncryptionMetadata.dataKey()).isEqualTo(DATA_KEY);
            assertThat(segmentEncryptionMetadata.aad()).isEqualTo(AAD);
        });
        assertThat(manifest.remoteLogSegmentMetadata()).isEqualTo(REMOTE_LOG_SEGMENT_METADATA);
    }
}
