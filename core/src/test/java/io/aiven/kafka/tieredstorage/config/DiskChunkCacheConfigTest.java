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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.aiven.kafka.tieredstorage.config.DiskChunkCacheConfig.CACHE_DIRECTORY;
import static io.aiven.kafka.tieredstorage.config.DiskChunkCacheConfig.TEMP_CACHE_DIRECTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class DiskChunkCacheConfigTest {

    @TempDir
    private Path path;
    private Path tempCachePath;
    private Path cachePath;

    @BeforeEach
    void setUp() throws IOException {
        tempCachePath = Files.createDirectories(path.resolve(TEMP_CACHE_DIRECTORY));
        cachePath = Files.createDirectories(path.resolve(CACHE_DIRECTORY));
    }

    @Test
    void validConfig() {
        final var config = new DiskChunkCacheConfig(
            Map.of(
                "size", "-1",
                "path", path.toString()
            )
        );
        assertThat(config.baseCachePath()).isEqualTo(path);
    }

    @Test
    void nonEmptyCachePath() throws IOException {
        Files.createFile(tempCachePath.resolve("temp-file"));
        Files.createFile(cachePath.resolve("cached-file"));

        final var config = new DiskChunkCacheConfig(
            Map.of(
                "size", "-1",
                "path", path.toString()
            )
        );
        assertThat(config.baseCachePath())
            .isEqualTo(path)
            .isDirectoryContaining(cp -> cp.equals(cachePath))
            .isDirectoryContaining(tcp -> tcp.equals(tempCachePath));
        assertThat(cachePath).isEmptyDirectory();
        assertThat(tempCachePath).isEmptyDirectory();
    }

    @Test
    void failedToResetCachePath() throws IOException {
        Files.createFile(tempCachePath.resolve("temp-file"));
        final var file = Files.createFile(cachePath.resolve("cached-file"));

        try (final var filesMockedStatic = mockStatic(FileUtils.class, CALLS_REAL_METHODS)) {
            filesMockedStatic.when(() -> FileUtils.cleanDirectory(eq(path.toFile())))
                .thenThrow(new IOException("Failed to delete file " + file));
            assertThat(path).exists();
            assertThatThrownBy(() -> new DiskChunkCacheConfig(
                Map.of(
                    "size", "-1",
                    "path", path.toString()
                )
            )).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value " + path + " for configuration path: "
                    + "Failed to reset cache directory, please empty the directory, reason: "
                    + "java.io.IOException: Failed to delete file " + file);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidBaseDirs")
    void invalidBaseDir(final File baseDir) {
        assertThatThrownBy(() -> new DiskChunkCacheConfig(
            Map.of(
                "size", "-1",
                "path", baseDir.toString()
            )))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value " + baseDir + " for configuration path: "
                + baseDir + " must exists and be a writable directory");
    }

    public static Stream<Arguments> invalidBaseDirs() throws IOException {
        return Stream.of(
            arguments(Named.of("non-existing directory", new File("non-existing"))),
            arguments(Named.of(
                "read-only directory",
                Files.createTempDirectory(
                    "path",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("---------"))).toFile())
            ),
            arguments(Named.of("not directory", Files.createTempFile("test", "").toFile()))
        );
    }
}
