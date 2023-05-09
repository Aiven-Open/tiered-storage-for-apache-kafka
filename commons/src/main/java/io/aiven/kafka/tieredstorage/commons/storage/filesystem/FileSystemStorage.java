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

import io.aiven.kafka.tieredstorage.commons.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.FileUploader;

import org.apache.commons.io.file.PathUtils;
import org.apache.commons.io.input.BoundedInputStream;

class FileSystemStorage implements FileUploader, FileFetcher, FileDeleter {
    private final Path fsRoot;
    private final boolean overwrites;

    FileSystemStorage(final String fsRoot, final boolean overwrites) {
        this(Path.of(fsRoot), overwrites);
    }

    FileSystemStorage(final Path fsRoot, final boolean overwrites) {
        if (!Files.isDirectory(fsRoot) || !Files.isWritable(fsRoot)) {
            throw new IllegalArgumentException(fsRoot + " must be a writable directory");
        }
        this.fsRoot = fsRoot;
        this.overwrites = overwrites;
    }

    @Override
    public void upload(final InputStream inputStream, final String key) throws IOException {
        final Path path = fsRoot.resolve(key);
        if (!overwrites && Files.exists(path)) {
            throw new IOException("File " + path + " already exists");
        }
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
    public InputStream fetch(final String key, final int from, final int to) throws IOException {
        if (from < 0) {
            throw new IllegalArgumentException("from cannot be negative, " + from + " given");
        }
        if (from > to) {
            throw new IllegalArgumentException("from cannot be more than to, from=" + from + ", to=" + to + " given");
        }

        final Path path = fsRoot.resolve(key);
        final long fileSize = Files.size(path);
        if (to > fileSize) {
            throw new IllegalArgumentException("position 'to' cannot be equal or higher than the file size, to="
                + to + ", file size=" + fileSize + " given");
        }

        final InputStream result = new BoundedInputStream(Files.newInputStream(path), to);
        result.skip(from);
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
}
