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

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.storage.BytesRange;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentIndexesV1BuilderTest {

    @Test
    void shouldFailWhenBuildingWithoutIndexes() {
        assertThatThrownBy(() -> new SegmentIndexesV1Builder().build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not enough indexes have been added; at least 4 required. Indexes included: []");
    }

    @Test
    void shouldFailWhenBuildingWithNotEnoughIndexes() {
        assertThatThrownBy(() -> new SegmentIndexesV1Builder()
            .add(RemoteStorageManager.IndexType.OFFSET, 1)
            .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not enough indexes have been added; at least 4 required. "
                + "Indexes included: [OFFSET, TIMESTAMP]");
    }

    @Test
    void shouldFailWhenAddingIndexManyTimes() {
        assertThatThrownBy(() -> new SegmentIndexesV1Builder()
            .add(RemoteStorageManager.IndexType.OFFSET, 1)
            .add(RemoteStorageManager.IndexType.OFFSET, 1)
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Index OFFSET is already added");
    }

    @Test
    void shouldBuildIndexesWithTransaction() {
        final var indexes = new SegmentIndexesV1Builder()
            .add(RemoteStorageManager.IndexType.OFFSET, 1)
            .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
            .add(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, 1)
            .add(RemoteStorageManager.IndexType.LEADER_EPOCH, 1)
            .add(RemoteStorageManager.IndexType.TRANSACTION, 1)
            .build();
        assertThat(indexes.offset()).isEqualTo(new SegmentIndexV1(0, 1));
        assertThat(indexes.timestamp()).isEqualTo(new SegmentIndexV1(1, 1));
        assertThat(indexes.producerSnapshot()).isEqualTo(new SegmentIndexV1(2, 1));
        assertThat(indexes.leaderEpoch()).isEqualTo(new SegmentIndexV1(3, 1));
        assertThat(indexes.transaction()).isEqualTo(new SegmentIndexV1(4, 1));
    }

    @Test
    void shouldBuildIndexesWithoutTransaction() {
        final var indexes = new SegmentIndexesV1Builder()
            .add(RemoteStorageManager.IndexType.OFFSET, 1)
            .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
            .add(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, 1)
            .add(RemoteStorageManager.IndexType.LEADER_EPOCH, 1)
            .build();
        assertThat(indexes.offset()).isEqualTo(new SegmentIndexV1(0, 1));
        assertThat(indexes.timestamp()).isEqualTo(new SegmentIndexV1(1, 1));
        assertThat(indexes.producerSnapshot()).isEqualTo(new SegmentIndexV1(2, 1));
        assertThat(indexes.leaderEpoch()).isEqualTo(new SegmentIndexV1(3, 1));
        assertThat(indexes.transaction()).isNull();
    }

    @Test
    void shouldBuildWithEmptyIndex() {
        final var indexes = new SegmentIndexesV1Builder()
            .add(RemoteStorageManager.IndexType.OFFSET, 0)
            .add(RemoteStorageManager.IndexType.TIMESTAMP, 1)
            .add(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, 0)
            .add(RemoteStorageManager.IndexType.LEADER_EPOCH, 1)
            .add(RemoteStorageManager.IndexType.TRANSACTION, 0)
            .build();
        assertThat(indexes.offset()).isEqualTo(new SegmentIndexV1(0, 0));
        assertThat(indexes.offset().range()).isEqualTo(BytesRange.ofFromPositionAndSize(0, 0));
        assertThat(indexes.timestamp()).isEqualTo(new SegmentIndexV1(0, 1));
        assertThat(indexes.producerSnapshot()).isEqualTo(new SegmentIndexV1(1, 0));
        assertThat(indexes.producerSnapshot().range()).isEqualTo(BytesRange.of(1, -1));
        assertThat(indexes.leaderEpoch()).isEqualTo(new SegmentIndexV1(1, 1));
        assertThat(indexes.transaction()).isEqualTo(new SegmentIndexV1(2, 0));
        assertThat(indexes.transaction().range()).isEqualTo(BytesRange.empty(2));
    }
}
