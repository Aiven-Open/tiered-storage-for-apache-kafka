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

package io.aiven.kafka.tieredstorage.storage;

import java.util.OptionalInt;

/**
 * Byte range with from and to edges; where `to` cannot be less than `from`
 * --unless to represent empty range where to is -1.
 * Both, `from` and `to`, are inclusive positions.
 */
public class BytesRange {
    final int from;
    final int to;

    BytesRange(final int from, final int to) {
        if (from < 0) {
            throw new IllegalArgumentException("from cannot be negative, " + from + " given");
        }
        if (to != -1 && to < from) {
            throw new IllegalArgumentException("to cannot be less than from, from=" + from + ", to=" + to + " given");
        }
        this.from = from;
        this.to = to;
    }

    public int firstPosition() {
        return from;
    }

    /**
     * @return empty if size == 0, otherwise last position (inclusive)
     */
    public OptionalInt maybeLastPosition() {
        if (isEmpty()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(to);
    }

    public int lastPosition() {
        if (isEmpty()) {
            throw new IllegalStateException("No last position, range is empty");
        }
        return to;
    }

    public boolean isEmpty() {
        return to == -1;
    }

    public int size() {
        if (isEmpty()) {
            return 0;
        }
        return to - from + 1;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BytesRange that = (BytesRange) o;
        return from == that.from && to == that.to;
    }

    @Override
    public int hashCode() {
        int result = from;
        result = 31 * result + to;
        return result;
    }

    @Override
    public String toString() {
        return "BytesRange{"
            + "position=" + firstPosition()
            + ", size=" + size()
            + '}';
    }

    public static BytesRange of(final int from, final int to) {
        return new BytesRange(from, to);
    }

    public static BytesRange empty(final int from) {
        return new BytesRange(from, -1);
    }

    public static BytesRange ofFromPositionAndSize(final int from, final int size) {
        if (size == 0) {
            return empty(from);
        }
        return new BytesRange(from, from + size - 1);
    }

}
