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

package io.aiven.kafka.tieredstorage.manifest.index;

import io.aiven.kafka.tieredstorage.Chunk;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunkEqualsTest {
    @Test
    void identical() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(0, 20, 30, 100, 120);
        assertThat(c1).isEqualTo(c2);
        assertThat(c2).isEqualTo(c1);
        assertThat(c1).hasSameHashCodeAs(c2);
    }

    @Test
    void differentIndex() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(1, 20, 30, 100, 120);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c2).isNotEqualTo(c1);
        assertThat(c1).doesNotHaveSameHashCodeAs(c2);
    }

    @Test
    void differentOriginalPosition() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(0, 21, 30, 100, 120);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c2).isNotEqualTo(c1);
        assertThat(c1).doesNotHaveSameHashCodeAs(c2);
    }

    @Test
    void differentOriginalSize() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(0, 20, 31, 100, 120);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c2).isNotEqualTo(c1);
        assertThat(c1).doesNotHaveSameHashCodeAs(c2);
    }

    @Test
    void differentTransformedPosition() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(0, 20, 30, 101, 120);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c2).isNotEqualTo(c1);
        assertThat(c1).doesNotHaveSameHashCodeAs(c2);
    }

    @Test
    void differentTransformedSize() {
        final var c1 = new Chunk(0, 20, 30, 100, 120);
        final var c2 = new Chunk(0, 20, 30, 100, 121);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c2).isNotEqualTo(c1);
        assertThat(c1).doesNotHaveSameHashCodeAs(c2);
    }
}
