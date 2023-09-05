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

import java.util.TreeMap;

import org.apache.kafka.common.protocol.types.SchemaException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentCustomMetadataSerdeTest {
    final SegmentCustomMetadataSerde serde = new SegmentCustomMetadataSerde();

    @Test
    void shouldSerDeEmptyFields() {
        final var bytes = serde.serialize(new TreeMap<>());
        assertThat(bytes).isEqualTo(new byte[] {});
        final var fields = serde.deserialize(bytes);
        assertThat(fields).isEmpty();
    }

    @Test
    void shouldSerDeSomeFields() {
        final var input = new TreeMap<Integer, Object>();
        input.put(0, 100L); // remote_size
        input.put(2, "foo"); // object_key

        final var bytes = serde.serialize(input);
        assertThat(bytes).hasSize(11); // calculated on the first run
        final var fields = serde.deserialize(bytes);
        assertThat(fields).hasSize(2)
            .containsEntry(0, 100L)
            .containsEntry(2, "foo");
    }

    @Test
    void shouldFailWhenWrongType() {
        final var input = new TreeMap<Integer, Object>();
        input.put(0, "foo"); // remote_size with wrong type

        assertThatThrownBy(() -> serde.serialize(input))
            .isInstanceOf(SchemaException.class);
    }

    @Test
    void shouldFailWhenUnknownLocation() {
        final var input = new TreeMap<Integer, Object>();
        input.put(SegmentCustomMetadataField.values().length + 1, "foo"); // unknown location

        assertThatThrownBy(() -> serde.serialize(input))
            .isInstanceOf(SchemaException.class);
    }
}
