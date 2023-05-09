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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemStorageFactoryTest {
    @Test
    void testNotAllowOverwriting(@TempDir final Path tempDir) throws IOException {
        final File fsRoot = tempDir.toFile();
        final ObjectStorageFactory osFactory = new FileSystemStorageFactory();
        osFactory.configure(Map.of(
            "root", fsRoot.toString(),
            "overwrite.enabled", "false"
        ));

        final InputStream logFile = new ByteArrayInputStream("log file".getBytes());
        osFactory.fileUploader().upload(logFile, "aaa/0.log.txt");

        assertThatThrownBy(() -> osFactory.fileUploader().upload(logFile, "aaa/0.log.txt"))
            .isInstanceOf(IOException.class)
            .hasMessage("File %s already exists", fsRoot + "/aaa/0.log.txt");
    }

    @Test
    void testAllowOverwriting(@TempDir final Path tempDir) throws IOException {
        final File fsRoot = tempDir.toFile();
        final ObjectStorageFactory osFactory = new FileSystemStorageFactory();
        osFactory.configure(Map.of(
            "root", fsRoot.toString(),
            "overwrite.enabled", "true"
        ));

        final byte[] data1 = "log file 1".getBytes();
        osFactory.fileUploader().upload(new ByteArrayInputStream(data1), "aaa/0.log.txt");
        assertThat(Files.readAllBytes(Path.of(fsRoot.getPath(), "aaa/0.log.txt")))
            .isEqualTo(data1);

        final byte[] data2 = "log file 2".getBytes();
        assertThatNoException().isThrownBy(
            () -> osFactory.fileUploader().upload(new ByteArrayInputStream(data2), "aaa/0.log.txt"));
        assertThat(Files.readAllBytes(Path.of(fsRoot.getPath(), "aaa/0.log.txt")))
            .isEqualTo(data2);
    }

    @Test
    void testUploadFetchDelete(@TempDir final Path tempDir) throws IOException {
        final File fsRoot = tempDir.toFile();
        final ObjectStorageFactory osFactory = new FileSystemStorageFactory();
        osFactory.configure(Map.of(
            "root", fsRoot.toString()
        ));

        final byte[] data = "some file".getBytes();
        final InputStream file = new ByteArrayInputStream(data);
        osFactory.fileUploader().upload(file, "aaa/0.log.txt");

        try (final InputStream fetch = osFactory.fileFetcher().fetch("aaa/0.log.txt")) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("some file");
        }

        try (final InputStream fetch = osFactory.fileFetcher().fetch("aaa/0.log.txt", 1, data.length - 2)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fi");
        }

        osFactory.fileDeleter().delete("aaa/0.log.txt");

        assertThat(new File(fsRoot, "aaa")).doesNotExist();
    }
}
