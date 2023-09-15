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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidUrlTest {

    private final ConfigDef.Validator urlValidator = new ValidUrl();

    @Test
    void invalidScheme() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "ftp://host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value ftp://host for configuration test: "
                + "URL must have scheme from the list [http, https]");
    }

    @Test
    void invalidHost() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value host for configuration test: Must be a valid URL");
    }

    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "https"})
    void validSchemes(final String scheme) {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", scheme + "://host"));
    }
}
