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

package io.aiven.kafka.tieredstorage.commons.storage.filesystem;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemStorageConfigTest {
    @Test
    void minimalConfig() {
        final FileSystemStorageConfig config = new FileSystemStorageConfig(Map.of(
            "root", "."
        ));
        assertThat(config.root()).isEqualTo(".");
        assertThat(config.overwrites()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void overwriteEnabledExplicit(final boolean overwriteEnabled) {
        final FileSystemStorageConfig config = new FileSystemStorageConfig(Map.of(
            "root", ".",
            "overwrite.enabled", Boolean.toString(overwriteEnabled)
        ));
        assertThat(config.overwrites()).isEqualTo(overwriteEnabled);
    }
}
