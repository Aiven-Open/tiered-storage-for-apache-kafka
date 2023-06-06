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

package io.aiven.kafka.tieredstorage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunkTest {
    @Test
    void rangeIsInclusive() {
        final Chunk chunk = new Chunk(0, 0, 10, 0, 12);

        assertThat(chunk.range().from).isEqualTo(0);
        assertThat(chunk.range().to).isEqualTo(11);
        assertThat(chunk.range().size()).isEqualTo(12);
    }
}
