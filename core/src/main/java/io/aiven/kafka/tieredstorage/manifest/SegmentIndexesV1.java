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

import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentIndexesV1 implements SegmentIndexes {

    private final SegmentIndexV1 offset;
    private final SegmentIndexV1 timestamp;
    private final SegmentIndexV1 producerSnapshot;
    private final SegmentIndexV1 leaderEpoch;
    private final SegmentIndexV1 transaction;

    @JsonCreator
    public SegmentIndexesV1(
        @JsonProperty(value = "offset", required = true) final SegmentIndexV1 offset,
        @JsonProperty(value = "timestamp", required = true) final SegmentIndexV1 timestamp,
        @JsonProperty(value = "producerSnapshot", required = true) final SegmentIndexV1 producerSnapshot,
        @JsonProperty(value = "leaderEpoch", required = true) final SegmentIndexV1 leaderEpoch,
        @JsonProperty(value = "transaction", required = true) final SegmentIndexV1 transaction
    ) {
        this.offset = Objects.requireNonNull(offset, "offset cannot be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp cannot be null");
        this.producerSnapshot = Objects.requireNonNull(producerSnapshot, "producerSnapshot cannot be null");
        this.leaderEpoch = Objects.requireNonNull(leaderEpoch, "leaderEpoch cannot be null");
        this.transaction = transaction;
    }

    public static SegmentIndexesV1Builder builder() {
        return new SegmentIndexesV1Builder();
    }

    @Override
    @JsonProperty("offset")
    public SegmentIndex offset() {
        return offset;
    }

    @Override
    @JsonProperty("timestamp")
    public SegmentIndex timestamp() {
        return timestamp;
    }

    @Override
    @JsonProperty("producerSnapshot")
    public SegmentIndex producerSnapshot() {
        return producerSnapshot;
    }

    @Override
    @JsonProperty("leaderEpoch")
    public SegmentIndex leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    @JsonProperty("transaction")
    public SegmentIndex transaction() {
        return transaction;
    }

    @Override
    public SegmentIndex segmentIndex(final RemoteStorageManager.IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return offset;
            case TIMESTAMP:
                return timestamp;
            case PRODUCER_SNAPSHOT:
                return producerSnapshot;
            case LEADER_EPOCH:
                return leaderEpoch;
            case TRANSACTION:
                return transaction;
            default:
                throw new IllegalArgumentException("Unknown index type");
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SegmentIndexesV1 that = (SegmentIndexesV1) o;
        return Objects.equals(offset, that.offset)
            && Objects.equals(timestamp, that.timestamp)
            && Objects.equals(producerSnapshot, that.producerSnapshot)
            && Objects.equals(leaderEpoch, that.leaderEpoch)
            && Objects.equals(transaction, that.transaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, producerSnapshot, leaderEpoch, transaction);
    }

    @Override
    public String toString() {
        return "SegmentIndexesV1{"
            + "offset=" + offset
            + ", timestamp=" + timestamp
            + ", producerSnapshot=" + producerSnapshot
            + ", leaderEpoch=" + leaderEpoch
            + ", transaction=" + transaction
            + '}';
    }
}
