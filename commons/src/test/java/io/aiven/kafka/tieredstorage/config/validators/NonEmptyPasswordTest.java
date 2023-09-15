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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NonEmptyPasswordTest {

    private final ConfigDef.Validator nonEmptyPasswordValidator = new NonEmptyPassword();

    @Test
    void emptyPassword() {
        assertThatThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password("   ")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("password value must not be empty");
    }

    @Test
    void nullPassword() {
        assertThatNoException().isThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", null));
    }

    @Test
    void validPassword() {
        assertThatNoException()
            .isThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password("pass")));
    }

    @Test
    void nullPasswordValue() {
        assertThatThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password(null)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("password value must not be empty");
    }
}
