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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemStorageTest {

    static final String TOPIC_PARTITION_SEGMENT_KEY = "topic/partition/log";

    @TempDir
    Path root;

    @Test
    void testRootCannotBeAFile() throws IOException {
        final Path wrongRoot = root.resolve("file_instead");
        Files.writeString(wrongRoot, "Wrong root");

        assertThatThrownBy(() -> new FileSystemStorage(wrongRoot, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(wrongRoot + " must be a writable directory");
    }

    @Test
    void testRootCannotBeNonWritableDirectory() throws IOException {
        final Path nonWritableDir = root.resolve("non_writable");
        Files.createDirectory(nonWritableDir).toFile().setReadOnly();

        assertThatThrownBy(() -> new FileSystemStorage(nonWritableDir, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(nonWritableDir + " must be a writable directory");
    }

    @Test
    void testUploadANewFile() throws IOException {
        final FileSystemStorage storage = new FileSystemStorage(root, false);
        final String content = "content";
        storage.upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(content).isEqualTo(Files.readString(root.resolve(TOPIC_PARTITION_SEGMENT_KEY)));
    }

    @Test
    void testUploadFailsWhenFileExists() throws IOException {
        final Path previous = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(previous.getParent());
        Files.writeString(previous, "previous");
        final FileSystemStorage storage = new FileSystemStorage(root, false);
        final String content = "content";

        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(content.getBytes()),
            TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(IOException.class)
            .hasMessage("File " + previous + " already exists");
    }

    @Test
    void testUploadWithOverridesWhenFileExists() throws IOException {
        final Path previous = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(previous.getParent());
        Files.writeString(previous, "previous");
        final FileSystemStorage storage = new FileSystemStorage(root, true);
        final String content = "content";
        storage.upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        assertThat(content).isEqualTo(Files.readString(root.resolve(TOPIC_PARTITION_SEGMENT_KEY)));
    }

    @Test
    void testFetchAll() throws IOException {
        final String content = "content";
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, content);
        final FileSystemStorage storage = new FileSystemStorage(root, true);

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            assertThat(content.getBytes()).isEqualTo(fetch.readAllBytes());
        }
    }

    @Test
    void testFetchWithOffsetRange() throws IOException {
        final String content = "AABBBBAA";
        final int from = 2;
        final int to = 6;
        final String range = content.substring(from, to + 1); // exclusive
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, content);
        final FileSystemStorage storage = new FileSystemStorage(root, true);

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, from, to)) {
            assertThat(range.getBytes()).isEqualTo(fetch.readAllBytes());
        }
    }

    @Test
    void testFetchWithFailsWithWrongOffsets() {
        final FileSystemStorage storage = new FileSystemStorage(root, true);

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, -1, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("from cannot be negative, -1 given");
        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, 2, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("from cannot be more than to, from=2, to=1 given");
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException {
        final String content = "content";
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, content);
        final FileSystemStorage storage = new FileSystemStorage(root, true);

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, 0, content.length() + 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("to cannot be bigger than file size, to=8, file size=7 given");
    }

    @Test
    void testDelete() throws IOException {
        final Path keyPath = root.resolve(TOPIC_PARTITION_SEGMENT_KEY);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage(root, false);
        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(Files.exists(keyPath)).isFalse(); // segment key
        assertThat(Files.exists(keyPath.getParent())).isFalse(); // partition dir
        assertThat(Files.exists(keyPath.getParent().getParent())).isFalse(); // topic dir
        assertThat(Files.exists(root)).isTrue();
    }

    @Test
    void testDeleteDoesNotRemoveParentDir() throws IOException {
        final String parent = "parent";
        final String key = "key";
        final Path parentPath = root.resolve(parent);
        Files.createDirectories(parentPath);
        Files.writeString(parentPath.resolve("another"), "test");
        final Path keyPath = parentPath.resolve(key);
        Files.writeString(keyPath, "test");
        final FileSystemStorage storage = new FileSystemStorage(root, false);
        storage.delete(parent + "/" + key);

        assertThat(Files.exists(keyPath)).isFalse();
        assertThat(Files.exists(parentPath)).isTrue();
        assertThat(Files.exists(root)).isTrue();
    }
}
