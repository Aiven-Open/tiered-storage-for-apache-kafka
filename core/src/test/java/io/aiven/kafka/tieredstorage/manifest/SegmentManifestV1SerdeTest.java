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

package io.aiven.kafka.tieredstorage.manifest;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;
import java.util.Map;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.security.EncryptedDataKey;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentManifestV1SerdeTest extends RsaKeyAwareTest {
    static final FixedSizeChunkIndex INDEX =
        new FixedSizeChunkIndex(100, 1000, 110, 110);
    static final SecretKey DATA_KEY = new SecretKeySpec(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "AES");
    static final byte[] AAD = {10, 11, 12, 13};

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

    static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();

    static final SegmentIndexesV1 SEGMENT_INDEXES_WITHOUT_TXN_INDEX = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .build();

    static final String REMOTE_LOG_SEGMENT_METADATA_JSON = "{"
        + "\"remoteLogSegmentId\":{"
        + "\"topicIdPartition\":{\"topicId\":\"lZ6vvmajTWKDBUTV6SQAtQ\",\"topicPartition\":"
        + "{\"topic\":\"topic1\",\"partition\":42}},"
        + "\"id\":\"adh9f8BMS4anaUnD8KWfWg\"},"
        + "\"startOffset\":0,\"endOffset\":1000,"
        + "\"maxTimestampMs\":1000000000,\"brokerId\":2,\"eventTimestampMs\":2000000000,"
        + "\"segmentLeaderEpochs\":{\"0\":100,\"1\":200,\"2\":300}}";

    static final String WITH_ENCRYPTION_WITHOUT_SECRET_KEY_JSON =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"segmentIndexes\":{"
            + "\"offset\":{\"position\":0,\"size\":1},"
            + "\"timestamp\":{\"position\":1,\"size\":1},"
            + "\"producerSnapshot\":{\"position\":2,\"size\":1},"
            + "\"leaderEpoch\":{\"position\":3,\"size\":1},"
            + "\"transaction\":{\"position\":4,\"size\":1}"
            + "},"
            + "\"compression\":false,\"encryption\":{\"aad\":\"CgsMDQ==\"},\"remoteLogSegmentMetadata\":"
            + REMOTE_LOG_SEGMENT_METADATA_JSON
            + "}";
    static final String WITHOUT_ENCRYPTION_JSON =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"segmentIndexes\":{"
            + "\"offset\":{\"position\":0,\"size\":1},"
            + "\"timestamp\":{\"position\":1,\"size\":1},"
            + "\"producerSnapshot\":{\"position\":2,\"size\":1},"
            + "\"leaderEpoch\":{\"position\":3,\"size\":1},"
            + "\"transaction\":{\"position\":4,\"size\":1}"
            + "},"
            + "\"compression\":false,\"remoteLogSegmentMetadata\":"
            + REMOTE_LOG_SEGMENT_METADATA_JSON
            + "}";

    static final String WITHOUT_ENCRYPTION_WITHOUT_TXN_INDEX_JSON =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"segmentIndexes\":{"
            + "\"offset\":{\"position\":0,\"size\":1},"
            + "\"timestamp\":{\"position\":1,\"size\":1},"
            + "\"producerSnapshot\":{\"position\":2,\"size\":1},"
            + "\"leaderEpoch\":{\"position\":3,\"size\":1},"
            + "\"transaction\":null"
            + "},"
            + "\"compression\":false,\"remoteLogSegmentMetadata\":"
            + REMOTE_LOG_SEGMENT_METADATA_JSON
            + "}";

    ObjectMapper mapper;
    RsaEncryptionProvider rsaEncryptionProvider;

    @BeforeEach
    void init() {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(KafkaTypeSerdeModule.create());

        rsaEncryptionProvider = new RsaEncryptionProvider(KEY_ENCRYPTION_KEY_ID, keyRing);
        mapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));
    }

    @Test
    void withEncryption() throws JsonProcessingException {
        final SegmentManifest manifest = new SegmentManifestV1(INDEX, SEGMENT_INDEXES, false,
            new SegmentEncryptionMetadataV1(DATA_KEY, AAD), REMOTE_LOG_SEGMENT_METADATA);

        final String jsonStr = mapper.writeValueAsString(manifest);

        // Check that the key is encrypted.
        final ObjectNode deserializedJson = (ObjectNode) mapper.readTree(jsonStr);
        final String dataKeyText = deserializedJson.get("encryption").get("dataKey").asText();
        final String[] dataKeyTextParts = dataKeyText.split(":");
        assertThat(dataKeyTextParts).hasSize(2);
        assertThat(dataKeyTextParts[0]).isEqualTo(KEY_ENCRYPTION_KEY_ID);
        final byte[] encryptedKey = Base64.getDecoder().decode(dataKeyTextParts[1]);
        final SecretKeySpec dataKey = new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(
            new EncryptedDataKey(KEY_ENCRYPTION_KEY_ID, encryptedKey)), "AES");
        assertThat(dataKey).isEqualTo(DATA_KEY);

        // Remove the secret key--i.e. the variable part--and compare the JSON representation.
        ((ObjectNode) deserializedJson.get("encryption")).remove("dataKey");
        assertThat(mapper.writeValueAsString(deserializedJson)).isEqualTo(WITH_ENCRYPTION_WITHOUT_SECRET_KEY_JSON);

        // Check deserialization.
        final SegmentManifest deserializedManifest = mapper.readValue(jsonStr, SegmentManifest.class);
        assertThat(deserializedManifest).isEqualTo(manifest);
    }

    @Test
    void withoutEncryption() throws JsonProcessingException {
        final var manifest = new SegmentManifestV1(INDEX, SEGMENT_INDEXES, false, null, REMOTE_LOG_SEGMENT_METADATA);

        final String jsonStr = mapper.writeValueAsString(manifest);

        // Compare the JSON representation.
        assertThat(jsonStr).isEqualTo(WITHOUT_ENCRYPTION_JSON);

        // Check deserialization.
        final SegmentManifest deserializedManifest = mapper.readValue(jsonStr, SegmentManifest.class);
        assertThat(deserializedManifest).isEqualTo(manifest);
    }

    @Test
    void withoutTxnIndex() throws JsonProcessingException {
        final var manifest = new SegmentManifestV1(INDEX, SEGMENT_INDEXES_WITHOUT_TXN_INDEX,
            false, null, REMOTE_LOG_SEGMENT_METADATA);

        final String jsonStr = mapper.writeValueAsString(manifest);

        // Compare the JSON representation.
        assertThat(jsonStr).isEqualTo(WITHOUT_ENCRYPTION_WITHOUT_TXN_INDEX_JSON);

        // Check deserialization.
        final SegmentManifest deserializedManifest = mapper.readValue(jsonStr, SegmentManifest.class);
        assertThat(deserializedManifest).isEqualTo(manifest);
    }
}
