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

package io.aiven.kafka.tieredstorage.config;

import java.io.InputStream;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

public class NoopStorageBackend implements StorageBackend {
    public boolean configureCalled = false;
    public Config configuredWith;

    @Override
    public void configure(final Map<String, ?> configs) {
        configureCalled = true;
        this.configuredWith = new Config(configs);
    }

    @Override
    public long upload(final InputStream inputStream, final String key) throws StorageBackendException {
        return 0;
    }

    @Override
    public InputStream fetch(final String key) throws StorageBackendException {
        return null;
    }

    @Override
    public InputStream fetch(final String key, final BytesRange range) throws StorageBackendException {
        return null;
    }

    @Override
    public void delete(final String key) throws StorageBackendException {
    }


    static class Config extends AbstractConfig {

        static ConfigDef config = new ConfigDef()
            .define("config1", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "config1")
            .define("config2", ConfigDef.Type.INT, -1, ConfigDef.Importance.MEDIUM, "config2")
            .define("config3", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "config3");

        public Config(final Map<String, ?> configs) {
            super(config, configs);
        }
    }
}
