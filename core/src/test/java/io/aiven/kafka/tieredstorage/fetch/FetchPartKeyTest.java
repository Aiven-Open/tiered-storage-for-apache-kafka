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

package io.aiven.kafka.tieredstorage.fetch;

import org.apache.kafka.common.Uuid;

import io.aiven.kafka.tieredstorage.storage.BytesRange;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FetchPartKeyTest {
    static final String UUID_1 = "topic/" + Uuid.randomUuid();
    static final String UUID_2 = "topic/" + Uuid.randomUuid();

    @Test
    void identical() {
        final var key1 = new FetchPartKey(UUID_1,  BytesRange.of(0, 9));
        final var key2 = new FetchPartKey(UUID_1, BytesRange.of(0, 9));
        assertThat(key1).isEqualTo(key2);
        assertThat(key2).isEqualTo(key1);
        assertThat(key1).hasSameHashCodeAs(key2);
    }

    @Test
    void differentUuid() {
        final var key1 = new FetchPartKey(UUID_1, BytesRange.of(0, 9));
        final var key2 = new FetchPartKey(UUID_2, BytesRange.of(0, 9));
        assertThat(key1).isNotEqualTo(key2);
        assertThat(key2).isNotEqualTo(key1);
        assertThat(key1).doesNotHaveSameHashCodeAs(key2);
    }

    @Test
    void differentChunkIds() {
        final var key1 = new FetchPartKey(UUID_1, BytesRange.of(0, 9));
        final var key2 = new FetchPartKey(UUID_1, BytesRange.of(10, 19));
        assertThat(key1).isNotEqualTo(key2);
        assertThat(key2).isNotEqualTo(key1);
        assertThat(key1).doesNotHaveSameHashCodeAs(key2);
    }

    @Test
    void singlePath() {
        assertThat(new FetchPartKey("test", BytesRange.of(0, 9)).path()).isEqualTo("test-0-9");
    }

    @Test
    void pathWitDir() {
        assertThat(new FetchPartKey("parent/test", BytesRange.of(0, 9)).path()).isEqualTo("test-0-9");
    }
}
