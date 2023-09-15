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

package io.aiven.kafka.tieredstorage.config.validators;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubclassTest {
    @Test
    void validSubclass() {
        assertThatNoException().isThrownBy(() -> Subclass.of(Object.class).ensureValid("test", String.class));
    }

    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> Subclass.of(Object.class).ensureValid("test", null));
    }

    @Test
    void invalidSubclass() {
        assertThatThrownBy(() -> Subclass.of(String.class).ensureValid("test", Object.class))
            .isInstanceOf(ConfigException.class)
            .hasMessage("test should be a subclass of java.lang.String");
    }
}
