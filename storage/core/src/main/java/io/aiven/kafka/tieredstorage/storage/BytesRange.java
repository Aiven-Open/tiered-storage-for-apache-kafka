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

/**
 * Byte range with from and to edges; where to cannot be less than from.
 */
public class BytesRange {
    public final int from;
    public final int to;

    BytesRange(final int from, final int to) {
        if (from < 0) {
            throw new IllegalArgumentException("from cannot be negative, " + from + " given");
        }
        if (to < from) {
            throw new IllegalArgumentException("to cannot be less than from, from=" + from + ", to=" + to + " given");
        }
        this.from = from;
        this.to = to;
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
        return "BytesRange("
            + "from=" + from
            + ", to=" + to
            + ")";
    }

    public static BytesRange of(final int from, final int to) {
        return new BytesRange(from, to);
    }
}
