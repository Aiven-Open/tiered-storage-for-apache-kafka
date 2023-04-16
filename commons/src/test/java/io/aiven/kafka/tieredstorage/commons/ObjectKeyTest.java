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

import java.util.Map;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeyTest {
    @Test
    void test() {
        final Uuid topicId = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
        final TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, new TopicPartition("topic", 7));
        final Uuid segmentId = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
        final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentId);
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
            remoteLogSegmentId, 1234L, 2000L,
            0, 0, 0, 0, Map.of(0, 0L));

        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LOG))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.log");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.OFFSET_INDEX))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.index");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.TIME_INDEX))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.timeindex");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.PRODUCER_SNAPSHOT))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.snapshot");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.TXN_INDEX))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.txnindex");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.LEADER_EPOCH_CHECKPOINT))
            .isEqualTo(
                "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.leader-epoch-checkpoint");
        assertThat(ObjectKey.key(remoteLogSegmentMetadata, ObjectKey.Suffix.MANIFEST))
            .isEqualTo("topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000001234-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest");
    }
}
