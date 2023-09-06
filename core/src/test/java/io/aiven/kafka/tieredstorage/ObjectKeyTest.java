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

import java.util.Map;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeyTest {
    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7));
    static final RemoteLogSegmentId REMOTE_LOG_SEGMENT_ID = new RemoteLogSegmentId(TOPIC_ID_PARTITION, SEGMENT_ID);
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA = new RemoteLogSegmentMetadata(
        REMOTE_LOG_SEGMENT_ID, 1234L, 2000L,
        0, 0, 0, 0, Map.of(0, 0L));

    @Test
    void test() {
        final ObjectKey objectKey = new ObjectKey("prefix/");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.MANIFEST))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsEmpty() {
        final ObjectKey objectKey = new ObjectKey("prefix/");
        final Map<Integer, Object> fields = Map.of();
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.MANIFEST))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyPrefix() {
        final ObjectKey objectKey = new ObjectKey("prefix/");
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.MANIFEST))
            .isEqualTo(
                "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyKey() {
        final ObjectKey objectKey = new ObjectKey("prefix/");
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo("prefix/topic/7/file.log");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo("prefix/topic/7/file.index");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo("prefix/topic/7/file.timeindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo("prefix/topic/7/file.snapshot");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo("prefix/topic/7/file.txnindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo("prefix/topic/7/file.leader-epoch-checkpoint");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.MANIFEST))
            .isEqualTo("prefix/topic/7/file.rsm-manifest");
    }

    @Test
    void withCustomFieldsAll() {
        final ObjectKey objectKey = new ObjectKey("prefix/");
        final Map<Integer, Object> fields = Map.of(
            SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/",
            SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo("other/topic/7/file.log");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo("other/topic/7/file.index");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo("other/topic/7/file.timeindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo("other/topic/7/file.snapshot");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo("other/topic/7/file.txnindex");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo("other/topic/7/file.leader-epoch-checkpoint");
        assertThat(objectKey.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.MANIFEST))
            .isEqualTo("other/topic/7/file.rsm-manifest");
    }

    @Test
    void nullPrefix() {
        final ObjectKey objectKey = new ObjectKey(null);
        assertThat(objectKey.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKey.Suffix.LOG))
            .isEqualTo(
                "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void suffixForIndexTypes() {
        assertThat(ObjectKey.Suffix.fromIndexType(RemoteStorageManager.IndexType.OFFSET))
            .isEqualTo(ObjectKey.Suffix.OFFSET_INDEX)
            .extracting("value")
            .isEqualTo("index");
        assertThat(ObjectKey.Suffix.fromIndexType(RemoteStorageManager.IndexType.TIMESTAMP))
            .isEqualTo(ObjectKey.Suffix.TIME_INDEX)
            .extracting("value")
            .isEqualTo("timeindex");
        assertThat(ObjectKey.Suffix.fromIndexType(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT))
            .isEqualTo(ObjectKey.Suffix.PRODUCER_SNAPSHOT)
            .extracting("value")
            .isEqualTo("snapshot");
        assertThat(ObjectKey.Suffix.fromIndexType(RemoteStorageManager.IndexType.TRANSACTION))
            .isEqualTo(ObjectKey.Suffix.TXN_INDEX)
            .extracting("value")
            .isEqualTo("txnindex");
        assertThat(ObjectKey.Suffix.fromIndexType(RemoteStorageManager.IndexType.LEADER_EPOCH))
            .isEqualTo(ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT)
            .extracting("value")
            .isEqualTo("leader-epoch-checkpoint");
    }
}
