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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.apache.commons.io.file.PathUtils;
import org.apache.commons.io.input.BoundedInputStream;

public class FileSystemStorage implements StorageBackend {

    private Path fsRoot;

    @Override
    public void configure(final Map<String, ?> configs) {
        final FileSystemStorageConfig config = new FileSystemStorageConfig(configs);
        this.fsRoot = config.root();
        if (!Files.isDirectory(fsRoot) || !Files.isWritable(fsRoot)) {
            throw new IllegalArgumentException(fsRoot + " must be a writable directory");
        }
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        try {
            final Path path = fsRoot.resolve(key.value());
            Files.createDirectories(path.getParent());
            Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
            return Files.size(path);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        try {
            final Path path = fsRoot.resolve(key.value());
            return Files.newInputStream(path);
        } catch (final NoSuchFileException e) {
            throw new KeyNotFoundException(this, key);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }

            final Path path = fsRoot.resolve(key.value());
            final long fileSize = Files.size(path);
            if (range.firstPosition() >= fileSize) {
                throw new InvalidRangeException("Range start position " + range.firstPosition()
                    + " is outside file content. file size = " + fileSize);
            }
            // slice file content
            final InputStream chunkContent = Files.newInputStream(path);
            chunkContent.skip(range.firstPosition());
            final long size = Math.min(range.lastPosition(), fileSize) - range.firstPosition() + 1;
            return new BoundedInputStream(chunkContent, size);
        } catch (final NoSuchFileException e) {
            throw new KeyNotFoundException(this, key);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key + ", with range " + range, e);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final Path path = fsRoot.resolve(key.value());
            Files.deleteIfExists(path);
            Path parent = path.getParent();
            while (parent != null && Files.isDirectory(parent) && !parent.equals(fsRoot)
                && PathUtils.isEmptyDirectory(parent)) {
                Files.deleteIfExists(parent);
                parent = parent.getParent();
            }
        } catch (final IOException e) {
            throw new StorageBackendException("Error when deleting " + key, e);
        }
    }

    @Override
    public String toString() {
        return "FileSystemStorage{"
            + "fsRoot=" + fsRoot + '}';
    }
}
