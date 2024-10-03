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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {@link org.apache.kafka.common.config.ConfigDef.Validator} implementation that verifies that a config value is
 * a valid URL with http or https schemes.
 */
public class ValidUrl implements ConfigDef.Validator {
    private static final List<String> SUPPORTED_SCHEMAS = List.of("http", "https");

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            try {
                final var url = new URL((String) value);
                if (!SUPPORTED_SCHEMAS.contains(url.getProtocol())) {
                    throw new ConfigException(name, value, "URL must have scheme from the list " + SUPPORTED_SCHEMAS);
                }
            } catch (final MalformedURLException e) {
                throw new ConfigException(name, value, "Must be a valid URL");
            }
        }
    }

    @Override
    public String toString() {
        return "Valid URL as defined in rfc2396";
    }
}
