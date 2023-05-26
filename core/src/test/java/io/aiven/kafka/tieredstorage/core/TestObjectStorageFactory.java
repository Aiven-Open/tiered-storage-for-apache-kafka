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

package io.aiven.kafka.tieredstorage.core;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.core.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.core.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.core.storage.FileUploader;
import io.aiven.kafka.tieredstorage.core.storage.ObjectStorageFactory;

public class TestObjectStorageFactory implements ObjectStorageFactory {
    public boolean configureCalled = false;
    public Config configuredWith;

    @Override
    public void configure(final Map<String, ?> configs) {
        configureCalled = true;
        this.configuredWith = new Config(configs);
    }

    @Override
    public FileUploader fileUploader() {
        return null;
    }

    @Override
    public FileFetcher fileFetcher() {
        return null;
    }

    @Override
    public FileDeleter fileDeleter() {
        return null;
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
