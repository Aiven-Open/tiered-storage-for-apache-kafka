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

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.kafka.common.protocol.types.Struct;

import static io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField.CUSTOM_METADATA_SCHEMA;
import static io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField.TAGGED_FIELD_NAME;

/**
 * Serialize and deserialize {@code NavigableMap} based on fields defined by {@code SegmentCustomMetadataField}.
 *
 * <p>{@code NavigableMap} is required by {@link org.apache.kafka.common.protocol.types.TaggedFields},
 * therefore is enforced on this API.
 */
public class SegmentCustomMetadataSerde {

    public byte[] serialize(final NavigableMap<Integer, Object> data) {
        if (data.isEmpty()) {
            return new byte[] {};
        }

        final var struct = new Struct(CUSTOM_METADATA_SCHEMA);
        struct.set(TAGGED_FIELD_NAME, data);

        final var buf = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buf);
        return buf.array();
    }

    public NavigableMap<Integer, Object> deserialize(final byte[] data) {
        if (data == null || data.length == 0) {
            return new TreeMap<>();
        }

        final var buf = ByteBuffer.wrap(data);
        final var struct = CUSTOM_METADATA_SCHEMA.read(buf);

        @SuppressWarnings("unchecked") final var fields =
            (NavigableMap<Integer, Object>) struct.get(TAGGED_FIELD_NAME);
        return fields;
    }
}
