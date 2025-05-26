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

package io.aiven.kafka.tieredstorage.manifest;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SegmentFormat {
    KAFKA,
    ICEBERG;

    @JsonValue
    public String getValue() {
        return name().toLowerCase();
    }

    @JsonCreator
    public static SegmentFormat fromValue(final String value) {
        final String normalizedValue = value.toLowerCase();
        for (final SegmentFormat format : values()) {
            if (format.getValue().equals(normalizedValue)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown segment format: " + value);
    }

    public static String[] allowedConfigValues() {
        return Arrays.stream(SegmentFormat.values())
            .map(SegmentFormat::name)
            .map(String::toLowerCase)
            .toArray(String[]::new);
    }
}
