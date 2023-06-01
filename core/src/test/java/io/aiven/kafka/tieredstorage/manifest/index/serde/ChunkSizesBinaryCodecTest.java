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

package io.aiven.kafka.tieredstorage.manifest.index.serde;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkSizesBinaryCodecTest {
    @Test
    void empty() {
        final List<Integer> values = List.of();
        final var encoded = ChunkSizesBinaryCodec.encode(values);
        final var decoded = ChunkSizesBinaryCodec.decode(encoded);
        assertThat(decoded).isEqualTo(values);
    }

    @Test
    void singleValue() {
        final List<Integer> values = List.of(213);
        final var encoded = ChunkSizesBinaryCodec.encode(values);
        final var decoded = ChunkSizesBinaryCodec.decode(encoded);
        assertThat(decoded).isEqualTo(values);
    }

    @Test
    void singleMaxInt() {
        final List<Integer> values = List.of(Integer.MAX_VALUE);
        final var encoded = ChunkSizesBinaryCodec.encode(values);
        final var decoded = ChunkSizesBinaryCodec.decode(encoded);
        assertThat(decoded).isEqualTo(values);
    }

    @ParameterizedTest
    @MethodSource("provideMultipleValues")
    void multipleValues(final List<Integer> values, final int expectedBytesPerValue) {
        final byte[] encoded = ChunkSizesBinaryCodec.encode(values);

        final int expectedTotalLength = 4 + 4 + 1 + (values.size() - 1) * expectedBytesPerValue + 4;
        assertThat(encoded.length).isEqualTo(expectedTotalLength);

        final ByteBuffer buf = ByteBuffer.wrap(encoded);
        // Check count.
        assertThat(buf.getInt()).isEqualTo(values.size());
        // Skip base.
        buf.getInt();
        // Check bytes per value.
        assertThat(buf.get()).isEqualTo((byte) expectedBytesPerValue);

        final var decoded = ChunkSizesBinaryCodec.decode(encoded);
        assertThat(decoded).isEqualTo(values);
    }

    static Stream<Arguments> provideMultipleValues() {
        final List<Integer> longList = new ArrayList<>();
        for (int i = 0; i < Integer.MAX_VALUE - 2000; i += 1000) {
            longList.add(i);
        }
        final List<Integer> longRevertedList = new ArrayList<>(longList);
        Collections.reverse(longRevertedList);

        return Stream.of(
            Arguments.of(List.of(0, 1000, 2, 44002, 369), 2),
            Arguments.of(List.of(Integer.MAX_VALUE, Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 2, 10), 1),
            Arguments.of(List.of(Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 2 - 1, Integer.MAX_VALUE / 2 - 2, 10), 1),
            Arguments.of(longList, 4),
            Arguments.of(longRevertedList, 4),
            // Explicit for 1 byte per value.
            Arguments.of(List.of(1, 2, 3, Integer.MAX_VALUE), 1),
            // Explicit for 2 bytes  per value.
            Arguments.of(List.of(1, 0xFF + 10, 0xFF + 20, 0xFF + 30, Integer.MAX_VALUE), 2),
            // Explicit for 3 bytes  per value.
            Arguments.of(List.of(1, 0xFFFF + 10, 0xFFFF + 20, 0xFFFF + 30, Integer.MAX_VALUE), 3),
            // Explicit for 4 bytes  per value.
            Arguments.of(List.of(1, 0xFFFFFF + 10, 0xFFFFFF + 20, 0xFFFFFF + 30, Integer.MAX_VALUE), 4)
        );
    }

    @ParameterizedTest
    @MethodSource("provideNegativeValues")
    void negativeValues(final List<Integer> values) {
        assertThatThrownBy(() -> ChunkSizesBinaryCodec.encode(values))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Values cannot be negative");
    }

    static Stream<List<Integer>> provideNegativeValues() {
        return Stream.of(
            List.of(-1),
            List.of(-1, 2, 3),
            List.of(1, -2, 3),
            List.of(1, 2, -3)
        );
    }
}
