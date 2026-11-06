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

package io.aiven.kafka.tieredstorage.iceberg.kafka;

import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.header.Header;

public final class KafkaHeader implements Header {
    private final String key;
    private final byte[] value;

    public KafkaHeader(final String key, final byte[] value) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof Header other && key.equals(other.key()) && Arrays.equals(value, other.value());
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return key + "=" + (value == null ? "null" : value.length + " bytes");
    }
}
