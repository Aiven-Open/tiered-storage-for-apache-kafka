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

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} implementation that verifies
 * that a config value is a subclass of a specified class.
 */
public class Subclass implements ConfigDef.Validator {
    private final Class<?> parentClass;

    public static Subclass of(final Class<?> parentClass) {
        return new Subclass(parentClass);
    }

    public Subclass(final Class<?> parentClass) {
        this.parentClass = parentClass;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null && !(parentClass.isAssignableFrom((Class<?>) value))) {
            throw new ConfigException(name + " should be a subclass of " + parentClass.getCanonicalName());
        }
    }

}
