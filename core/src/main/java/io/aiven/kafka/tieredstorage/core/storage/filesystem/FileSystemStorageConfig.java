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

package io.aiven.kafka.tieredstorage.core.storage.filesystem;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class FileSystemStorageConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    private static final String ROOT_CONFIG = "root";
    private static final String ROOT_DOC = "Root directory";

    private static final String OVERWRITE_ENABLED_CONFIG = "overwrite.enabled";
    private static final String OVERWRITE_ENABLED_DOC = "Enable overwriting existing files";

    static {
        CONFIG = new ConfigDef();
        CONFIG.define(
            ROOT_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            ROOT_DOC
        );
        CONFIG.define(
            OVERWRITE_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            OVERWRITE_ENABLED_DOC
        );
    }

    FileSystemStorageConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    final Path root() {
        return Path.of(getString(ROOT_CONFIG));
    }
}
