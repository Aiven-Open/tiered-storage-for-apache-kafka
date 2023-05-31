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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.commons.io.FileUtils;

public class DiskBasedChunkCacheConfig extends ChunkCacheConfig {
    private static final String CACHE_PATH_CONFIG = "path";
    private static final String CACHE_PATH_DOC = "Cache directory";

    public static final String TEMP_CACHE_DIRECTORY = "temp";
    public static final String CACHE_DIRECTORY = "cache";

    private static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        configDef.define(
                CACHE_PATH_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                CACHE_PATH_DOC
        );
        return configDef;
    }

    public DiskBasedChunkCacheConfig(final Map<String, ?> props) {
        super(configDef(), props);
        // Cleaning the cache directory since there is no way so far
        // to reuse previously cached files after broker restart.
        resetCacheDirectory();
    }

    private void resetCacheDirectory() {
        if (Files.exists(baseCachePath())) {
            try {
                FileUtils.deleteDirectory(baseCachePath().toFile());
                Files.createDirectories(cachePath());
                Files.createDirectories(tempCachePath());
            } catch (final IOException e) {
                throw new ConfigException(CACHE_PATH_CONFIG, baseCachePath(),
                        "Failed to reset cache directory, please empty the directory, reason: " + e.getMessage());
            }
        }
    }

    final Path baseCachePath() {
        return Path.of(getString(CACHE_PATH_CONFIG));
    }

    final Path cachePath() {
        return baseCachePath().resolve(CACHE_DIRECTORY);
    }

    final Path tempCachePath() {
        return baseCachePath().resolve(TEMP_CACHE_DIRECTORY);
    }
}
