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

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

public class NonEmptyPassword implements ConfigDef.Validator {
    @Override
    public void ensureValid(final String name, final Object value) {
        if (Objects.isNull(value)) {
            return;
        }
        final var pwd = (Password) value;
        if (pwd.value() == null || pwd.value().isBlank()) {
            throw new ConfigException(name + " value must not be empty");
        }
    }
}
