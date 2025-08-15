/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.iceberg.data;

import java.util.List;

import org.apache.avro.Schema;

public abstract class RowSchema {
    public interface Fields {
        String KAFKA = "kafka";
        String PARTITION = "partition";
        String OFFSET = "offset";
        String TIMESTAMP = "timestamp";
        String BATCH_BYTE_OFFSET = "batch_byte_offset";
        String BATCH_BASE_OFFSET = "batch_base_offset";
        String BATCH_PARTITION_LEADER_EPOCH = "batch_partition_leader_epoch";
        String BATCH_MAGIC = "batch_magic";
        String BATCH_TIMESTAMP_TYPE = "batch_timestamp_type";
        String BATCH_COMPRESSION_TYPE = "batch_compression_type";
        String BATCH_MAX_TIMESTAMP = "batch_max_timestamp";
        String BATCH_PRODUCER_ID = "batch_producer_id";
        String BATCH_PRODUCER_EPOCH = "batch_producer_epoch";
        String BATCH_BASE_SEQUENCE = "batch_base_sequence";

        String HEADERS = "headers";
        String HEADER_KEY = "key";
        String HEADER_VALUE = "value";

        String KEY = "key";
        String KEY_RAW = "key_raw";
        String VALUE = "value";
        String VALUE_RAW = "value_raw";
    }

    public static final Schema KAFKA = Schema.createRecord(Fields.KAFKA, "", "", false, List.of(
        new Schema.Field(Fields.PARTITION, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.OFFSET, Schema.create(Schema.Type.LONG)),
        new Schema.Field(Fields.TIMESTAMP, Schema.create(Schema.Type.LONG)),
        new Schema.Field(Fields.BATCH_BYTE_OFFSET, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_BASE_OFFSET, Schema.create(Schema.Type.LONG)),
        new Schema.Field(Fields.BATCH_PARTITION_LEADER_EPOCH, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_MAGIC, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_TIMESTAMP_TYPE, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_COMPRESSION_TYPE, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_MAX_TIMESTAMP, Schema.create(Schema.Type.LONG)),
        new Schema.Field(Fields.BATCH_PRODUCER_ID, Schema.create(Schema.Type.LONG)),
        new Schema.Field(Fields.BATCH_PRODUCER_EPOCH, Schema.create(Schema.Type.INT)),
        new Schema.Field(Fields.BATCH_BASE_SEQUENCE, Schema.create(Schema.Type.INT))
    ));

    public static final Schema HEADER = Schema.createRecord(Fields.HEADERS, "", "", false, List.of(
        new Schema.Field(Fields.HEADER_KEY, Schema.create(Schema.Type.STRING)),
        new Schema.Field(Fields.HEADER_VALUE, Schema.create(Schema.Type.BYTES))
    ));
    public static final Schema HEADERS = Schema.createArray(HEADER);

    public static final Schema KEY_RAW =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    public static final Schema VALUE_RAW =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));

    public static Schema createRowSchema(final Schema keySchema, final Schema valueSchema) {
        return Schema.createRecord("record", "", "", false, List.of(
            new Schema.Field(Fields.KAFKA, KAFKA),
            new Schema.Field(Fields.HEADERS, HEADERS),
            new Schema.Field(Fields.KEY, keySchema),
            new Schema.Field(Fields.KEY_RAW, KEY_RAW),
            new Schema.Field(Fields.VALUE, valueSchema),
            new Schema.Field(Fields.VALUE_RAW, VALUE_RAW)
        ));
    }
}
