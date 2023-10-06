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

package io.aiven.kafka.tieredstorage.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.LEADER_EPOCH;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.OFFSET;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TIMESTAMP;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType.TRANSACTION;

/**
 * Defines the segment index fields to be collected together as a single object
 */
public enum SegmentIndex {
    OFFSET_INDEX(0,
        new Field(OFFSET.name(), Type.COMPACT_BYTES),
        segmentData -> {
            try {
                return ByteBuffer.wrap(Files.readAllBytes(segmentData.offsetIndex()));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }),
    TIME_INDEX(1,
        new Field(TIMESTAMP.name(), Type.COMPACT_BYTES),
        segmentData -> {
            try {
                return ByteBuffer.wrap(Files.readAllBytes(segmentData.timeIndex()));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }),
    PRODUCER_SNAPSHOT_INDEX(2,
        new Field(PRODUCER_SNAPSHOT.name(), Type.COMPACT_BYTES),
        segmentData -> {
            try {
                return ByteBuffer.wrap(Files.readAllBytes(segmentData.producerSnapshotIndex()));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }),
    LEADER_EPOCH_INDEX(3, new Field(LEADER_EPOCH.name(), Type.COMPACT_BYTES), LogSegmentData::leaderEpochIndex),
    TRANSACTION_INDEX(4,
        new Field(TRANSACTION.name(), Type.COMPACT_NULLABLE_BYTES),
        segmentData -> segmentData.transactionIndex()
            .map(i -> {
                try {
                    return ByteBuffer.wrap(Files.readAllBytes(i));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null));

    static final Field.TaggedFieldsSection FIELDS_SECTION = Field.TaggedFieldsSection.of(
        OFFSET_INDEX.index, OFFSET_INDEX.field,
        TIME_INDEX.index, TIME_INDEX.field,
        PRODUCER_SNAPSHOT_INDEX.index, PRODUCER_SNAPSHOT_INDEX.field,
        LEADER_EPOCH_INDEX.index, LEADER_EPOCH_INDEX.field,
        TRANSACTION_INDEX.index, TRANSACTION_INDEX.field
    );
    static final Map<String, Integer> KAFKA_MAPPING = Map.of(
        OFFSET.name(), OFFSET_INDEX.index,
        TIMESTAMP.name(), TIME_INDEX.index,
        PRODUCER_SNAPSHOT.name(), PRODUCER_SNAPSHOT_INDEX.index,
        LEADER_EPOCH.name(), LEADER_EPOCH_INDEX.index,
        TRANSACTION.name(), TRANSACTION_INDEX.index
    );
    public static final Schema SEGMENT_INDEXES_SCHEMA = new Schema(FIELDS_SECTION);
    public static final String TAGGED_FIELD_NAME = FIELDS_SECTION.name;

    final int index;
    final Field field;
    final Function<LogSegmentData, ByteBuffer> valueProvider;

    SegmentIndex(final int index,
                 final Field field,
                 final Function<LogSegmentData, ByteBuffer> valueProvider) {
        this.index = index;
        this.field = field;
        this.valueProvider = valueProvider;
    }

    public static NavigableMap<Integer, Object> build(final LogSegmentData segmentData) {
        final TreeMap<Integer, Object> taggedFields = new TreeMap<>();
        for (final var index: SegmentIndex.values()) {
            taggedFields.put(index.index, index.valueProvider.apply(segmentData));
        }
        return taggedFields;
    }

    public static Integer indexFrom(final RemoteStorageManager.IndexType indexType) {
        return KAFKA_MAPPING.get(indexType.name());
    }
}
