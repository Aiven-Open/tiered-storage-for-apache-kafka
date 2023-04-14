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

package io.aiven.kafka.tieredstorage.commons.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FixedSizeChunkIndexEqualsTest {
    @Test
    void identical() {
        final var i1 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        final var i2 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        assertThat(i1).isEqualTo(i2);
        assertThat(i2).isEqualTo(i1);
        assertThat(i1).hasSameHashCodeAs(i2);
    }
    
    @Test
    void differentOriginalChunkSize() {
        final var i1 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        final var i2 = new FixedSizeChunkIndex(101, 1000, 110, 10);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentOriginalFileSize() {
        final var i1 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        final var i2 = new FixedSizeChunkIndex(100, 1001, 110, 10);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentTransformedChunkSize() {
        final var i1 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        final var i2 = new FixedSizeChunkIndex(100, 1000, 111, 10);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentFinalTransformedChunkSize() {
        final var i1 = new FixedSizeChunkIndex(100, 1000, 110, 10);
        final var i2 = new FixedSizeChunkIndex(100, 1000, 110, 11);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }
}
