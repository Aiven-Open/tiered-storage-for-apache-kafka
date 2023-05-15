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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.FileUploader;
import io.aiven.kafka.tieredstorage.commons.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;

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
    public void upload(final InputStream inputStream, final String key) throws StorageBackEndException {
        try {
            final Path path = fsRoot.resolve(key);
            Files.createDirectories(path.getParent());
            try (final OutputStream outputStream = Files.newOutputStream(path)) {
                inputStream.transferTo(outputStream);
            }
        } catch (final IOException e) {
            throw new StorageBackEndException("Failed to upload " + key, e);
        }
    }

    @Override
    public InputStream fetch(final String key) throws StorageBackEndException {
        try {
            final Path path = fsRoot.resolve(key);
            return Files.newInputStream(path);
        } catch (final NoSuchFileException e) {
            throw new KeyNotFoundException(this, key, e);
        } catch (final IOException e) {
            throw new StorageBackEndException("Failed to fetch " + key, e);
        }
    }

    @Override
    public InputStream fetch(final String key, final BytesRange range) throws StorageBackEndException {
        try {
            final Path path = fsRoot.resolve(key);
            final long fileSize = Files.size(path);
            if (range.to > fileSize) {
                throw new IllegalArgumentException("position 'to' cannot be higher than the file size, to="
                    + range.to + ", file size=" + fileSize + " given");
            }

            final InputStream result = new BoundedInputStream(Files.newInputStream(path), range.to);
            result.skip(range.from);
            return result;
        } catch (final NoSuchFileException e) {
            throw new KeyNotFoundException(this, key, e);
        } catch (final IOException e) {
            throw new StorageBackEndException("Failed to fetch " + key + ", with range " + range, e);
        }
    }

    @Override
    public void delete(final String key) throws StorageBackEndException {
        try {
            final Path path = fsRoot.resolve(key);
            Files.deleteIfExists(path);
            Path parent = path.getParent();
            while (parent != null && Files.isDirectory(parent) && !parent.equals(fsRoot)
                && PathUtils.isEmptyDirectory(parent)) {
                Files.deleteIfExists(parent);
                parent = parent.getParent();
            }
        } catch (final IOException e) {
            throw new StorageBackEndException("Error when deleting " + key, e);
        }
    }

    @Override
    public String toString() {
        return "FileSystemStorage{"
            + "fsRoot=" + fsRoot + '}';
    }
}
