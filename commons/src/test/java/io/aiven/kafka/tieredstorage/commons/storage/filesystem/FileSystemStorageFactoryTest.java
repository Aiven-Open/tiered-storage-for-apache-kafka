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
import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemStorageFactoryTest {
    @Test
    void testUploadFetchDelete(@TempDir final Path tempDir) throws IOException, StorageBackEndException {
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

        final BytesRange range = BytesRange.of(1, data.length - 2);
        try (final InputStream fetch = osFactory.fileFetcher().fetch("aaa/0.log.txt", range)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fi");
        }

        osFactory.fileDeleter().delete("aaa/0.log.txt");

        assertThat(new File(fsRoot, "aaa")).doesNotExist();
    }
}
