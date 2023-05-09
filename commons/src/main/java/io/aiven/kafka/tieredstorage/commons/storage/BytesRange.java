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

package io.aiven.kafka.tieredstorage.commons.storage;

/**
 * Byte range with from and to edges; where to cannot be less than from.
 * inclusive/exclusive semantics are up to the consumer.
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

    public static BytesRange of(final int from, final int to) {
        return new BytesRange(from, to);
    }
}
