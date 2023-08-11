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

import java.util.EnumMap;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.ObjectKey;

public class SegmentCustomMetadataBuilder {
    final ObjectKey objectKey;
    final RemoteLogSegmentMetadata segmentMetadata;
    final EnumMap<ObjectKey.Suffix, Long> uploadResults;

    final Set<SegmentCustomMetadataField> fields;

    public SegmentCustomMetadataBuilder(final Set<SegmentCustomMetadataField> fields,
                                        final ObjectKey objectKey,
                                        final RemoteLogSegmentMetadata segmentMetadata) {
        this.fields = fields;
        this.objectKey = objectKey;
        this.segmentMetadata = segmentMetadata;
        this.uploadResults = new EnumMap<>(ObjectKey.Suffix.class);
    }

    public SegmentCustomMetadataBuilder addUploadResult(final ObjectKey.Suffix suffix,
                                                        final long bytes) {
        if (uploadResults.containsKey(suffix)) {
            throw new IllegalArgumentException("Upload results for suffix " + suffix + " already added");
        }
        uploadResults.put(suffix, bytes);
        return this;
    }

    public long totalSize() {
        return uploadResults.values().stream().mapToLong(value -> value).sum();
    }

    /**
     * {@code NavigableMap} is required by {@link org.apache.kafka.common.protocol.types.TaggedFields},
     * therefore is enforced on this API.
     */
    public NavigableMap<Integer, Object> build() {
        final TreeMap<Integer, Object> taggedFields = new TreeMap<>();
        fields.forEach(field -> taggedFields.put(field.ordinal(), field.valueProvider.apply(this)));
        return taggedFields;
    }
}
