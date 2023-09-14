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

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} implementation that is used to combine another validator
 * with allowing null as a valid value. Useful for cases like
 * <pre>
 * {@code Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE))}
 * </pre>
 * where existing validator that does not allow null values con be used but null is still a default value.
 */
public class Null implements ConfigDef.Validator {

    private final ConfigDef.Validator validator;

    public static ConfigDef.Validator or(final ConfigDef.Validator validator) {
        return new Null(validator);
    }

    private Null(final ConfigDef.Validator validator) {
        this.validator = validator;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            validator.ensureValid(name, value);
        }
    }
}

