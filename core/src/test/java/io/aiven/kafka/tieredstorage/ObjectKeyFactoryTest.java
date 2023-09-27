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

class ObjectKeyFactoryTest {
    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7));
    static final RemoteLogSegmentId REMOTE_LOG_SEGMENT_ID = new RemoteLogSegmentId(TOPIC_ID_PARTITION, SEGMENT_ID);
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA = new RemoteLogSegmentMetadata(
        REMOTE_LOG_SEGMENT_ID, 1234L, 2000L,
        0, 0, 0, 0, Map.of(0, 0L));

    @Test
    void test() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.OFFSET_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TIME_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TXN_INDEX))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST))
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsEmpty() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/");
        final Map<Integer, Object> fields = Map.of();
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.OFFSET_INDEX)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TIME_INDEX)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TXN_INDEX)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST)
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyPrefix() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/");
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.OFFSET_INDEX)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TIME_INDEX)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TXN_INDEX)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST)
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyKey() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/");
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).isEqualTo("prefix/topic/7/file.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.OFFSET_INDEX)
        ).isEqualTo("prefix/topic/7/file.index");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TIME_INDEX)
        ).isEqualTo("prefix/topic/7/file.timeindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT)
        ).isEqualTo("prefix/topic/7/file.snapshot");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TXN_INDEX)
        ).isEqualTo("prefix/topic/7/file.txnindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT)
        ).isEqualTo("prefix/topic/7/file.leader-epoch-checkpoint");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST)
        ).isEqualTo("prefix/topic/7/file.rsm-manifest");
    }

    @Test
    void withCustomFieldsAll() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/");
        final Map<Integer, Object> fields = Map.of(
            SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/",
            SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).isEqualTo("other/topic/7/file.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.OFFSET_INDEX)
        ).isEqualTo("other/topic/7/file.index");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TIME_INDEX)
        ).isEqualTo("other/topic/7/file.timeindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT)
        ).isEqualTo("other/topic/7/file.snapshot");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.TXN_INDEX)
        ).isEqualTo("other/topic/7/file.txnindex");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT)
        ).isEqualTo("other/topic/7/file.leader-epoch-checkpoint");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST)
        ).isEqualTo("other/topic/7/file.rsm-manifest");
    }

    @Test
    void nullPrefix() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory(null);
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG))
            .isEqualTo(
                "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void suffixForIndexTypes() {
        assertThat(ObjectKeyFactory.Suffix.fromIndexType(RemoteStorageManager.IndexType.OFFSET))
            .isEqualTo(ObjectKeyFactory.Suffix.OFFSET_INDEX)
            .extracting("value")
            .isEqualTo("index");
        assertThat(ObjectKeyFactory.Suffix.fromIndexType(RemoteStorageManager.IndexType.TIMESTAMP))
            .isEqualTo(ObjectKeyFactory.Suffix.TIME_INDEX)
            .extracting("value")
            .isEqualTo("timeindex");
        assertThat(ObjectKeyFactory.Suffix.fromIndexType(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT))
            .isEqualTo(ObjectKeyFactory.Suffix.PRODUCER_SNAPSHOT)
            .extracting("value")
            .isEqualTo("snapshot");
        assertThat(ObjectKeyFactory.Suffix.fromIndexType(RemoteStorageManager.IndexType.TRANSACTION))
            .isEqualTo(ObjectKeyFactory.Suffix.TXN_INDEX)
            .extracting("value")
            .isEqualTo("txnindex");
        assertThat(ObjectKeyFactory.Suffix.fromIndexType(RemoteStorageManager.IndexType.LEADER_EPOCH))
            .isEqualTo(ObjectKeyFactory.Suffix.LEADER_EPOCH_CHECKPOINT)
            .extracting("value")
            .isEqualTo("leader-epoch-checkpoint");
    }
}
