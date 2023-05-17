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

package io.aiven.kafka.tieredstorage.commons.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStorageFactoryTest {

    protected static final String TOPIC_PARTITION_SEGMENT_KEY = "topic/partition/log";

    protected abstract ObjectStorageFactory storageFactory();

    @Test
    void testUploadFetchDelete() throws IOException, StorageBackEndException {
        final byte[] data = "some file".getBytes();
        final InputStream file = new ByteArrayInputStream(data);
        storageFactory().fileUploader().upload(file, TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = storageFactory().fileFetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("some file");
        }

        final BytesRange range = BytesRange.of(1, data.length - 2);
        try (final InputStream fetch = storageFactory().fileFetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, range)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fi");
        }

        storageFactory().fileDeleter().delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storageFactory().fileFetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key topic/partition/log does not exists in storage "
                + storageFactory().fileFetcher());
    }
}
