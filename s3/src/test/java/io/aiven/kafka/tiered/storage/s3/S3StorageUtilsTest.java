/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tiered.storage.s3;

import java.util.Collections;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class S3StorageUtilsTest {

    private static final String TOPIC = "connect-log";

    @Test
    void getFileKey() {
        final TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        final Uuid id = Uuid.randomUuid();
        final RemoteLogSegmentId remoteLogSegmentId =
                new RemoteLogSegmentId(new TopicIdPartition(Uuid.randomUuid(), topicPartition), id);
        final String suffix = "log";

        final RemoteLogSegmentMetadata metadata =
                new RemoteLogSegmentMetadata(remoteLogSegmentId, 1, -1, -1, -1, 1L,
                        1, Collections.singletonMap(1, 100L));

        assertThat(S3StorageUtils.getFileKey(metadata, suffix))
                .isEqualTo(topicPartition + "/" + id + "-00000000000000000001." + suffix);

        final RemoteLogSegmentMetadata metadata2 =
                new RemoteLogSegmentMetadata(remoteLogSegmentId, Long.MAX_VALUE, -1, -1, -1, 1L,
                        1, Collections.singletonMap(1, 100L));

        assertThat(S3StorageUtils.getFileKey(metadata2, suffix))
                .isEqualTo(topicPartition + "/" + id + "-09223372036854775807." + suffix);
    }
}
