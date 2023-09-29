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

package io.aiven.kafka.tieredstorage.metadata;

import java.util.Collections;
import java.util.Set;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.ObjectKeyFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class SegmentCustomMetadataBuilderTest {
    static final Uuid TOPIC_ID = Uuid.randomUuid();
    static final Uuid SEGMENT_ID = Uuid.randomUuid();
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 0)),
                SEGMENT_ID),
            1, 100, -1, -1, 1L,
            100, Collections.singletonMap(1, 100L));
    static final ObjectKeyFactory OBJECT_KEY_FACTORY = new ObjectKeyFactory("p1", false);

    @Test
    void shouldBuildEmptyMap() {
        final var b = new SegmentCustomMetadataBuilder(Set.of(), OBJECT_KEY_FACTORY, REMOTE_LOG_SEGMENT_METADATA);
        assertThat(b.build()).isEmpty();

        // even when upload results are added
        b.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, 10L);
        assertThat(b.build()).isEmpty();
    }

    @Test
    void shouldFailWhenAddingExistingSuffixUploadResult() {
        final var b = new SegmentCustomMetadataBuilder(Set.of(), OBJECT_KEY_FACTORY, REMOTE_LOG_SEGMENT_METADATA);

        b.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, 10L);
        assertThatThrownBy(() -> b.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, 20L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Upload results for suffix MANIFEST already added");
    }

    @Test
    void shouldIncludeTotalSize() {
        final var field = SegmentCustomMetadataField.REMOTE_SIZE;
        final var b = new SegmentCustomMetadataBuilder(Set.of(field), OBJECT_KEY_FACTORY, REMOTE_LOG_SEGMENT_METADATA);
        var fields = b.build();
        assertThat(fields)
            .containsExactly(entry(field.index, 0L)); // i.e. no upload results

        // but, when existing upload results
        b
            .addUploadResult(ObjectKeyFactory.Suffix.LOG, 40L)
            .addUploadResult(ObjectKeyFactory.Suffix.MANIFEST, 2L);
        fields = b.build();
        assertThat(fields)
            .containsExactly(entry(field.index, 42L)); // i.e. sum
    }

    @Test
    void shouldIncludeObjectPrefix() {
        final var field = SegmentCustomMetadataField.OBJECT_PREFIX;
        final var b = new SegmentCustomMetadataBuilder(Set.of(field), OBJECT_KEY_FACTORY, REMOTE_LOG_SEGMENT_METADATA);
        final var fields = b.build();
        assertThat(fields)
            .containsExactly(entry(field.index, "p1"));
    }

    @Test
    void shouldIncludeObjectKey() {
        final var field = SegmentCustomMetadataField.OBJECT_KEY;
        final var b = new SegmentCustomMetadataBuilder(Set.of(field), OBJECT_KEY_FACTORY, REMOTE_LOG_SEGMENT_METADATA);
        final var fields = b.build();
        assertThat(fields)
            .containsExactly(entry(field.index, "topic-" + TOPIC_ID + "/0/00000000000000000001-" + SEGMENT_ID));
    }
}
