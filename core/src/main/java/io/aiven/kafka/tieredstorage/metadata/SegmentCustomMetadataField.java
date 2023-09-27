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

import java.util.Arrays;
import java.util.function.Function;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;

import io.aiven.kafka.tieredstorage.ObjectKeyFactory;

// index define values on custom metadata fields and cannot be changed without breaking compatibility.
public enum SegmentCustomMetadataField {
    REMOTE_SIZE(0, new Field("remote_size", Type.VARLONG), SegmentCustomMetadataBuilder::totalSize),
    OBJECT_PREFIX(1, new Field("object_prefix", Type.COMPACT_STRING), b -> b.objectKeyFactory.prefix()),
    OBJECT_KEY(2, new Field("object_key", Type.COMPACT_STRING), b -> ObjectKeyFactory.mainPath(b.segmentMetadata));

    static final TaggedFieldsSection FIELDS_SECTION = TaggedFieldsSection.of(
        REMOTE_SIZE.index, REMOTE_SIZE.field,
        OBJECT_PREFIX.index, OBJECT_PREFIX.field,
        OBJECT_KEY.index, OBJECT_KEY.field
    );
    public static final Schema CUSTOM_METADATA_SCHEMA = new Schema(FIELDS_SECTION);
    public static final String TAGGED_FIELD_NAME = FIELDS_SECTION.name;

    final int index;
    final Field field;
    final Function<SegmentCustomMetadataBuilder, Object> valueProvider;

    SegmentCustomMetadataField(final int index,
                               final Field field,
                               final Function<SegmentCustomMetadataBuilder, Object> valueProvider) {
        this.index = index;
        this.field = field;
        this.valueProvider = valueProvider;
    }

    public static String[] names() {
        return Arrays.stream(SegmentCustomMetadataField.values())
            .map(SegmentCustomMetadataField::name)
            .toArray(String[]::new);
    }

    public int index() {
        return index;
    }
}
