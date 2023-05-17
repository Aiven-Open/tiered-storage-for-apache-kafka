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

import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.commons.storage.BaseStorageFactoryTest;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;

import org.junit.jupiter.api.io.TempDir;

class FileSystemStorageFactoryTest extends BaseStorageFactoryTest {

    @TempDir
    Path root;

    @Override
    protected ObjectStorageFactory storageFactory() {
        final FileSystemStorageFactory fileSystemStorageFactory = new FileSystemStorageFactory();
        fileSystemStorageFactory.configure(Map.of(
            "root", root.toAbsolutePath().toString()
        ));
        return fileSystemStorageFactory;
    }
}
