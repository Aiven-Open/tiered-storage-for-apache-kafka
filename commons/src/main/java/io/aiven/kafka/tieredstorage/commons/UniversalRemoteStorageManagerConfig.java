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

package io.aiven.kafka.tieredstorage.commons;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;

public class UniversalRemoteStorageManagerConfig extends AbstractConfig {
    private static final String OBJECT_STORAGE_PREFIX = "object.storage.";

    private static final String OBJECT_STORAGE_FACTORY_CONFIG = OBJECT_STORAGE_PREFIX + "factory";
    private static final String OBJECT_STORAGE_FACTORY_DOC = "The factory of an object storage implementation";

    private static final String OBJECT_STORAGE_KEY_PREFIX_CONFIG = "key.prefix";
    private static final String OBJECT_STORAGE_KEY_PREFIX_DOC = "The object storage path prefix";

    private static final String CHUNK_SIZE_CONFIG = "chunk.size";
    private static final String CHUNK_SIZE_DOC = "The chunk size of log files";

    private static final String COMPRESSION_CONFIG = "compression.enabled";
    private static final String COMPRESSION_DOC = "Whether to enable compression";

    private static final String ENCRYPTION_CONFIG = "encryption.enabled";
    private static final String ENCRYPTION_DOC = "Whether to enable encryption";
    private static final String ENCRYPTION_PUBLIC_KEY_FILE_CONFIG = "encryption.public.key.file";
    private static final String ENCRYPTION_PUBLIC_KEY_FILE_DOC = "The path to the RSA public key file";
    private static final String ENCRYPTION_PRIVATE_KEY_FILE_CONFIG = "encryption.private.key.file";
    private static final String ENCRYPTION_PRIVATE_KEY_FILE_DOC = "The path to the RSA private key file";
    // TODO add possibility to pass keys as strings

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        // TODO checkers

        CONFIG.define(
            OBJECT_STORAGE_FACTORY_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            OBJECT_STORAGE_FACTORY_DOC
        );

        CONFIG.define(
            OBJECT_STORAGE_KEY_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            OBJECT_STORAGE_KEY_PREFIX_DOC
        );

        CONFIG.define(
            CHUNK_SIZE_CONFIG,
            ConfigDef.Type.INT,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            ConfigDef.Importance.HIGH,
            CHUNK_SIZE_DOC
        );

        CONFIG.define(
            COMPRESSION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            COMPRESSION_DOC
        );

        CONFIG.define(
            ENCRYPTION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_DOC
        );
        CONFIG.define(
            ENCRYPTION_PUBLIC_KEY_FILE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_PUBLIC_KEY_FILE_DOC
        );
        CONFIG.define(
            ENCRYPTION_PRIVATE_KEY_FILE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_PRIVATE_KEY_FILE_DOC
        );
    }


    UniversalRemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        validate();
    }

    private void validate() {
        if (getBoolean(ENCRYPTION_CONFIG) && getString(ENCRYPTION_PUBLIC_KEY_FILE_CONFIG) == null) {
            throw new ConfigException(
                ENCRYPTION_PUBLIC_KEY_FILE_CONFIG + " must be provided if encryption is enabled");
        }
        if (getBoolean(ENCRYPTION_CONFIG) && getString(ENCRYPTION_PRIVATE_KEY_FILE_CONFIG) == null) {
            throw new ConfigException(
                ENCRYPTION_PRIVATE_KEY_FILE_CONFIG + " must be provided if encryption is enabled");
        }
    }

    ObjectStorageFactory objectStorageFactory() {
        final ObjectStorageFactory objectFactory = Utils.newInstance(
            getClass(OBJECT_STORAGE_FACTORY_CONFIG), ObjectStorageFactory.class);
        objectFactory.configure(this.originalsWithPrefix(OBJECT_STORAGE_PREFIX));
        return objectFactory;
    }

    String keyPrefix() {
        return getString(OBJECT_STORAGE_KEY_PREFIX_CONFIG);
    }

    int chunkSize() {
        return getInt(CHUNK_SIZE_CONFIG);
    }

    boolean compressionEnabled() {
        return getBoolean(COMPRESSION_CONFIG);
    }

    boolean encryptionEnabled() {
        return getBoolean(ENCRYPTION_CONFIG);
    }

    Path encryptionPublicKeyFile() {
        final String value = getString(ENCRYPTION_PUBLIC_KEY_FILE_CONFIG);
        if (value == null) {
            return null;
        }
        return Path.of(value);
    }

    Path encryptionPrivateKeyFile() {
        final String value = getString(ENCRYPTION_PRIVATE_KEY_FILE_CONFIG);
        if (value == null) {
            return null;
        }
        return Path.of(value);
    }
}
