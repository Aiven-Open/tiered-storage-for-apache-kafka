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

package io.aiven.kafka.tiered.storage.commons.chunkmanager;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkKeyTest {
    static final Uuid UUID_1 = Uuid.randomUuid();
    static final Uuid UUID_2 = Uuid.randomUuid();

    @Test
    void identical() {
        final var ck1 = new ChunkKey(UUID_1, 0);
        final var ck2 = new ChunkKey(UUID_1, 0);
        assertThat(ck1).isEqualTo(ck2);
        assertThat(ck2).isEqualTo(ck1);
        assertThat(ck1).hasSameHashCodeAs(ck2);
    }

    @Test
    void differentUuid() {
        final var ck1 = new ChunkKey(UUID_1, 0);
        final var ck2 = new ChunkKey(UUID_2, 0);
        assertThat(ck1).isNotEqualTo(ck2);
        assertThat(ck2).isNotEqualTo(ck1);
        assertThat(ck1).doesNotHaveSameHashCodeAs(ck2);
    }

    @Test
    void differentChunkIds() {
        final var ck1 = new ChunkKey(UUID_1, 0);
        final var ck2 = new ChunkKey(UUID_1, 1);
        assertThat(ck1).isNotEqualTo(ck2);
        assertThat(ck2).isNotEqualTo(ck1);
        assertThat(ck1).doesNotHaveSameHashCodeAs(ck2);
    }
}
