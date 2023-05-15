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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.aiven.kafka.tieredstorage.commons.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.FileUploader;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemStorageTest extends BaseStorageTest {

    @TempDir
    Path root;

    @Override
    protected FileUploader uploader() {
        return new FileSystemStorage(root);
    }

    @Override
    protected FileFetcher fetcher() {
        return new FileSystemStorage(root);
    }

    @Override
    protected FileDeleter deleter() {
        return new FileSystemStorage(root);
    }

    @Test
    void testRootCannotBeAFile() throws IOException {
        final Path wrongRoot = root.resolve("file_instead");
        Files.writeString(wrongRoot, "Wrong root");

        assertThatThrownBy(() -> new FileSystemStorage(wrongRoot))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(wrongRoot + " must be a writable directory");
    }

    @Test
    void testRootCannotBeNonWritableDirectory() throws IOException {
        final Path nonWritableDir = root.resolve("non_writable");
        Files.createDirectory(nonWritableDir).toFile().setReadOnly();

        assertThatThrownBy(() -> new FileSystemStorage(nonWritableDir))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(nonWritableDir + " must be a writable directory");
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException {
        final String content = "content";
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, content);
        final FileSystemStorage storage = new FileSystemStorage(root);

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(0, content.length() + 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("position 'to' cannot be higher than the file size, to=8, file size=7 given");
    }

    @Test
    void testDeleteAllParentsButRoot() throws IOException, StorageBackEndException {
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage(root);
        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(keyPath).doesNotExist(); // segment key
        assertThat(keyPath.getParent()).doesNotExist(); // partition key
        assertThat(keyPath.getParent().getParent()).doesNotExist(); // partition key
        assertThat(root).exists();
    }

    @Test
    void testDeleteDoesNotRemoveParentDir() throws IOException, StorageBackEndException {
        final String parent = "parent";
        final String key = "key";
        final Path parentPath = root.resolve(parent);
        Files.createDirectories(parentPath);
        Files.writeString(parentPath.resolve("another"), "test");
        final Path keyPath = parentPath.resolve(key);
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage(root);
        storage.delete(parent + "/" + key);

        assertThat(keyPath).doesNotExist();
        assertThat(parentPath).exists();
        assertThat(root).exists();
    }
}
