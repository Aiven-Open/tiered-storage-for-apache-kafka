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

package io.aiven.kafka.tieredstorage.storage.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

public class HdfsStorage implements StorageBackend {

    private int uploadBufferSize;
    private String absoluteRootPath;

    private FileSystem fileSystem;

    @Override
    public void configure(final Map<String, ?> configs) {
        final HdfsStorageConfig config = new HdfsStorageConfig(configs);
        uploadBufferSize = config.uploadBufferSize();
        try {
            final Configuration hadoopConf = config.hadoopConf();
            config.hdfsAuthenticator().authenticate();

            fileSystem = FileSystem.get(hadoopConf);

            final Path rootDirectory = new Path(config.rootDirectory());
            validateRootDir(rootDirectory);
            fileSystem.setWorkingDirectory(rootDirectory);

            absoluteRootPath = fileSystem.makeQualified(rootDirectory).toString();
        } catch (final IOException exception) {
            throw new RuntimeException("Can't create Hadoop filesystem with provided config",
                exception);
        }
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key)
        throws StorageBackendException {

        final Path filePath = new Path(key.value());
        final Path containingDirectory = filePath.getParent();
        try {
            if (!fileSystem.exists(containingDirectory)) {
                fileSystem.mkdirs(containingDirectory);
            }

            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath, true)) {
                return IOUtils.copy(inputStream, fsDataOutputStream, uploadBufferSize);
            }
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        try {
            final Path path = new Path(key.value());
            return fileSystem.open(path);
        } catch (final FileNotFoundException e) {
            throw new KeyNotFoundException(this, key);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range)
        throws StorageBackendException {

        final Path path = new Path(key.value());
        try {
            final FSDataInputStream inputStream = fileSystem.open(path);
            final long fileSize = fileSystem.getFileStatus(path).getLen();
            if (range.from >= fileSize) {
                throw new InvalidRangeException("Range start position " + range.from
                    + " is outside file content. file size = " + fileSize);
            }

            inputStream.seek(range.from);
            final long size = Math.min(range.to, fileSize) - range.from + 1;
            return new BoundedInputStream(inputStream, size);
        } catch (final FileNotFoundException e) {
            throw new KeyNotFoundException(this, key);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key + ", with range " + range, e);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final Path path = new Path(key.value());
            fileSystem.delete(path, false);
        } catch (final IOException e) {
            throw new StorageBackendException("Error when deleting " + key, e);
        }
    }

    @Override
    public String toString() {
        return "HdfsStorage{"
            + "root='" + absoluteRootPath + '\''
            + '}';
    }

    private void validateRootDir(final Path path) throws IOException {
        try {
            if (!fileSystem.getFileStatus(path).isDirectory()) {
                throw new IllegalArgumentException(path + " must be a directory");
            }
            fileSystem.access(path, FsAction.WRITE);
        } catch (final FileNotFoundException exception) {
            fileSystem.mkdirs(path);
        } catch (final AccessDeniedException exception) {
            throw new IllegalArgumentException(path + "must be a writable directory");
        }
    }
}
