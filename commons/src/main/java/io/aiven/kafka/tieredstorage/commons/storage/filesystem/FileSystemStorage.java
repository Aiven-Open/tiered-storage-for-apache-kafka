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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.FileUploader;

import org.apache.commons.io.file.PathUtils;
import org.apache.commons.io.input.BoundedInputStream;

class FileSystemStorage implements FileUploader, FileFetcher, FileDeleter {
    private final Path fsRoot;

    FileSystemStorage(final Path fsRoot) {
        if (!Files.isDirectory(fsRoot) || !Files.isWritable(fsRoot)) {
            throw new IllegalArgumentException(fsRoot + " must be a writable directory");
        }
        this.fsRoot = fsRoot;
    }

    @Override
    public void upload(final InputStream inputStream, final String key) throws IOException {
        final Path path = fsRoot.resolve(key);
        Files.createDirectories(path.getParent());
        try (final OutputStream outputStream = Files.newOutputStream(path)) {
            inputStream.transferTo(outputStream);
        }
    }

    @Override
    public InputStream fetch(final String key) throws IOException {
        final Path path = fsRoot.resolve(key);
        return Files.newInputStream(path);
    }

    @Override
    public InputStream fetch(final String key, final BytesRange range) throws IOException {
        final Path path = fsRoot.resolve(key);
        final long fileSize = Files.size(path);
        final int size;
        if (range.to < fileSize) {
            size = range.to + 1; // inclusive end range
        } else {
            size = (int) fileSize;
        }

        final InputStream result = new BoundedInputStream(Files.newInputStream(path), size);
        result.skip(range.from);
        return result;
    }

    @Override
    public void delete(final String key) throws IOException {
        final Path path = fsRoot.resolve(key);
        Files.deleteIfExists(path);
        Path parent = path.getParent();
        while (parent != null && Files.isDirectory(parent) && !parent.equals(fsRoot)
            && PathUtils.isEmptyDirectory(parent)) {
            Files.deleteIfExists(parent);
            parent = parent.getParent();
        }
    }

    @Override
    public String toString() {
        return "FileSystemStorage{"
            + "fsRoot=" + fsRoot + '}';
    }
}
