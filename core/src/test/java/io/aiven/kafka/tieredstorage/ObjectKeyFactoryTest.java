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
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", false);
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value())
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES).value())
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.indexes");
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value())
            .isEqualTo(
                "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                    + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsEmpty() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", false);
        final Map<Integer, Object> fields = Map.of();
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value()
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES).value()
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.indexes");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value()
        ).isEqualTo(
            "prefix/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyPrefix() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", false);
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value()
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES).value()
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.indexes");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value()
        ).isEqualTo(
            "other/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/"
                + "00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }

    @Test
    void withCustomFieldsOnlyKey() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", false);
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value()
        ).isEqualTo("prefix/topic/7/file.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES).value()
        ).isEqualTo("prefix/topic/7/file.indexes");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value()
        ).isEqualTo("prefix/topic/7/file.rsm-manifest");
    }

    @Test
    void withCustomFieldsAll() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", false);
        final Map<Integer, Object> fields = Map.of(
            SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/",
            SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value()
        ).isEqualTo("other/topic/7/file.log");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.INDEXES).value()
        ).isEqualTo("other/topic/7/file.indexes");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value()
        ).isEqualTo("other/topic/7/file.rsm-manifest");
    }

    @Test
    void nullPrefix() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory(null, false);
        assertThat(objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG).value())
            .isEqualTo(
                "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void prefixMasking() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("prefix/", true);
        assertThat(
            objectKeyFactory.key(REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).hasToString(
            "<prefix>/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void prefixMaskingWithCustomFieldsEmpty() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("real-prefix/", true);
        final Map<Integer, Object> fields = Map.of();
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).hasToString(
            "<prefix>/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void prefixMaskingWithCustomFieldsOnlyPrefix() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("real-prefix/", true);
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).hasToString(
            "<prefix>/topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
    }

    @Test
    void prefixMaskingWithCustomFieldsOnlyKey() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("real-prefix/", true);
        final Map<Integer, Object> fields = Map.of(SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).hasToString("<prefix>/topic/7/file.log");
    }

    @Test
    void prefixMaskingWithCustomFieldsAll() {
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("real-prefix/", true);
        final Map<Integer, Object> fields = Map.of(
            SegmentCustomMetadataField.OBJECT_PREFIX.index(), "other/",
            SegmentCustomMetadataField.OBJECT_KEY.index(), "topic/7/file");
        assertThat(
            objectKeyFactory.key(fields, REMOTE_LOG_SEGMENT_METADATA, ObjectKeyFactory.Suffix.LOG)
        ).hasToString("<prefix>/topic/7/file.log");
    }
}
