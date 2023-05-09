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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BytesRangeTest {

    @Test
    void testProperRange() {
        final BytesRange range = BytesRange.of(1, 2);
        assertThat(range.from).isEqualTo(1);
        assertThat(range.to).isEqualTo(2);
    }

    @Test
    void testNegative() {
        assertThatThrownBy(() -> BytesRange.of(-1, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("from cannot be negative, -1 given");
    }

    @Test
    void testFromLargerThanTo() {
        assertThatThrownBy(() -> BytesRange.of(2, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("to cannot be less than from, from=2, to=1 given");
    }
}
