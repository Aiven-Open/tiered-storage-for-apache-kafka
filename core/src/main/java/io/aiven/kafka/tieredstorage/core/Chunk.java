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

package io.aiven.kafka.tieredstorage.core;

import io.aiven.kafka.tieredstorage.core.storage.BytesRange;

public class Chunk {
    public final int id;
    public final int originalPosition;
    public final int originalSize;
    public final int transformedPosition;
    public final int transformedSize;

    public Chunk(final int id,
                 final int originalPosition, final int originalSize,
                 final int transformedPosition, final int transformedSize) {
        this.id = id;
        this.originalPosition = originalPosition;
        this.originalSize = originalSize;
        this.transformedPosition = transformedPosition;
        this.transformedSize = transformedSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Chunk that = (Chunk) o;

        if (id != that.id) {
            return false;
        }
        if (originalPosition != that.originalPosition) {
            return false;
        }
        if (originalSize != that.originalSize) {
            return false;
        }
        if (transformedPosition != that.transformedPosition) {
            return false;
        }
        return transformedSize == that.transformedSize;
    }

    BytesRange range() {
        return BytesRange.of(transformedPosition, transformedPosition + transformedSize);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + originalPosition;
        result = 31 * result + originalSize;
        result = 31 * result + transformedPosition;
        result = 31 * result + transformedSize;
        return result;
    }

    @Override
    public String toString() {
        return "Chunk("
            + "index=" + id
            + ", originalPosition=" + originalPosition
            + ", originalSize=" + originalSize
            + ", transformedPosition=" + transformedPosition
            + ", transformedSize=" + transformedSize
            + ")";
    }
}
