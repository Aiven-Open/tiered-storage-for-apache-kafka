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

package io.aiven.kafka.tieredstorage.core.storage.filesystem;

import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.core.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.core.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.core.storage.FileUploader;
import io.aiven.kafka.tieredstorage.core.storage.ObjectStorageFactory;

public class FileSystemStorageFactory implements ObjectStorageFactory {
    private Path root;

    @Override
    public void configure(final Map<String, ?> configs) {
        final FileSystemStorageConfig config = new FileSystemStorageConfig(configs);
        this.root = config.root();
    }

    @Override
    public FileUploader fileUploader() {
        return new FileSystemStorage(root);
    }

    @Override
    public FileFetcher fileFetcher() {
        return new FileSystemStorage(root);
    }

    @Override
    public FileDeleter fileDeleter() {
        return new FileSystemStorage(root);
    }
}
