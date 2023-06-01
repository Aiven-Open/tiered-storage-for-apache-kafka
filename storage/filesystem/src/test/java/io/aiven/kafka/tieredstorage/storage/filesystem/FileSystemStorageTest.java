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

package io.aiven.kafka.tieredstorage.storage.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemStorageTest extends BaseStorageTest {

    @TempDir
    Path root;

    @Override
    protected StorageBackend storage() {
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(Map.of("root", root.toString()));
        return storage;
    }

    @Test
    void testRootCannotBeAFile() throws IOException {
        final Path wrongRoot = root.resolve("file_instead");
        Files.writeString(wrongRoot, "Wrong root");

        assertThatThrownBy(() -> {
            final FileSystemStorage storage = new FileSystemStorage();
            storage.configure(Map.of("root", wrongRoot.toString()));
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(wrongRoot + " must be a writable directory");
    }

    @Test
    void testRootCannotBeNonWritableDirectory() throws IOException {
        final Path nonWritableDir = root.resolve("non_writable");
        Files.createDirectory(nonWritableDir).toFile().setReadOnly();

        assertThatThrownBy(() -> {
            final FileSystemStorage storage = new FileSystemStorage();
            storage.configure(Map.of("root", nonWritableDir.toString()));
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(nonWritableDir + " must be a writable directory");
    }

    @Test
    void testDeleteAllParentsButRoot() throws IOException, StorageBackendException {
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(Map.of("root", root.toString()));
        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(keyPath).doesNotExist(); // segment key
        assertThat(keyPath.getParent()).doesNotExist(); // partition key
        assertThat(keyPath.getParent().getParent()).doesNotExist(); // partition key
        assertThat(root).exists();
    }

    @Test
    void testDeleteDoesNotRemoveParentDir() throws IOException, StorageBackendException {
        final String parent = "parent";
        final String key = "key";
        final Path parentPath = root.resolve(parent);
        Files.createDirectories(parentPath);
        Files.writeString(parentPath.resolve("another"), "test");
        final Path keyPath = parentPath.resolve(key);
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage();
        storage.configure(Map.of("root", root.toString()));
        storage.delete(parent + "/" + key);

        assertThat(keyPath).doesNotExist();
        assertThat(parentPath).exists();
        assertThat(root).exists();
    }
}
