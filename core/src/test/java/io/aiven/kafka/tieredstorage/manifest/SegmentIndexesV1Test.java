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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SegmentIndexesV1Test {

    @Test
    void identical() {
        final var offset = new SegmentIndexV1(100, 1000);
        final var timestamp = new SegmentIndexV1(100, 1000);
        final var producerSnapshot = new SegmentIndexV1(100, 1000);
        final var leaderEpoch = new SegmentIndexV1(100, 1000);
        final var transaction = new SegmentIndexV1(100, 1000);
        final var i1 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch, transaction);
        final var i2 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch, transaction);
        assertThat(i1).isEqualTo(i2);
        assertThat(i2).isEqualTo(i1);
        assertThat(i1).hasSameHashCodeAs(i2);
    }

    @Test
    void differentOffset() {
        final var offset1 = new SegmentIndexV1(100, 1000);
        final var offset2 = new SegmentIndexV1(101, 1001);
        final var timestamp = new SegmentIndexV1(100, 1000);
        final var producerSnapshot = new SegmentIndexV1(100, 1000);
        final var leaderEpoch = new SegmentIndexV1(100, 1000);
        final var transaction = new SegmentIndexV1(100, 1000);
        final var i1 = new SegmentIndexesV1(offset1, timestamp, producerSnapshot, leaderEpoch, transaction);
        final var i2 = new SegmentIndexesV1(offset2, timestamp, producerSnapshot, leaderEpoch, transaction);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentTimestamp() {
        final var offset = new SegmentIndexV1(100, 1000);
        final var timestamp1 = new SegmentIndexV1(100, 1000);
        final var timestamp2 = new SegmentIndexV1(101, 1001);
        final var producerSnapshot = new SegmentIndexV1(100, 1000);
        final var leaderEpoch = new SegmentIndexV1(100, 1000);
        final var transaction = new SegmentIndexV1(100, 1000);
        final var i1 = new SegmentIndexesV1(offset, timestamp1, producerSnapshot, leaderEpoch, transaction);
        final var i2 = new SegmentIndexesV1(offset, timestamp2, producerSnapshot, leaderEpoch, transaction);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentProducerSnapshot() {
        final var offset = new SegmentIndexV1(100, 1000);
        final var timestamp = new SegmentIndexV1(100, 1000);
        final var producerSnapshot1 = new SegmentIndexV1(100, 1000);
        final var producerSnapshot2 = new SegmentIndexV1(101, 1001);
        final var leaderEpoch = new SegmentIndexV1(100, 1000);
        final var transaction = new SegmentIndexV1(100, 1000);
        final var i1 = new SegmentIndexesV1(offset, timestamp, producerSnapshot1, leaderEpoch, transaction);
        final var i2 = new SegmentIndexesV1(offset, timestamp, producerSnapshot2, leaderEpoch, transaction);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentLeaderEpoch() {
        final var offset = new SegmentIndexV1(100, 1000);
        final var timestamp = new SegmentIndexV1(100, 1000);
        final var producerSnapshot = new SegmentIndexV1(100, 1000);
        final var leaderEpoch1 = new SegmentIndexV1(100, 1000);
        final var leaderEpoch2 = new SegmentIndexV1(101, 1001);
        final var transaction = new SegmentIndexV1(100, 1000);
        final var i1 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch1, transaction);
        final var i2 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch2, transaction);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentTransaction() {
        final var offset = new SegmentIndexV1(100, 1000);
        final var timestamp = new SegmentIndexV1(100, 1000);
        final var producerSnapshot = new SegmentIndexV1(100, 1000);
        final var leaderEpoch = new SegmentIndexV1(100, 1000);
        final var transaction1 = new SegmentIndexV1(100, 1000);
        final var transaction2 = new SegmentIndexV1(101, 1001);
        final var i1 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch, transaction1);
        final var i2 = new SegmentIndexesV1(offset, timestamp, producerSnapshot, leaderEpoch, transaction2);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }
}
