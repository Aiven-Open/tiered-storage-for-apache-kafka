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

import io.aiven.kafka.tieredstorage.storage.BytesRange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentIndexV1 implements SegmentIndex {
    private final int position;
    private final int size;

    @JsonCreator
    public SegmentIndexV1(@JsonProperty(value = "position", required = true) final int position,
                          @JsonProperty(value = "size", required = true) final int size) {
        this.position = position;
        this.size = size;
    }

    @Override
    @JsonProperty("position")
    public int position() {
        return position;
    }

    @Override
    @JsonProperty("size")
    public int size() {
        return size;
    }

    @Override
    public BytesRange range() {
        return BytesRange.ofFromPositionAndSize(position, size);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SegmentIndexV1 that = (SegmentIndexV1) o;
        return position == that.position && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, size);
    }

    @Override
    public String toString() {
        return "SegmentIndexV1{"
            + "position=" + position
            + ", size=" + size
            + '}';
    }
}
