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

package io.aiven.kafka.tieredstorage.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStorageTest {

    protected static final ObjectKey TOPIC_PARTITION_SEGMENT_KEY = new TestObjectKey("topic/partition/log");

    protected abstract StorageBackend storage();

    @Test
    void testUploadFetchDelete() throws IOException, StorageBackendException {
        final byte[] data = "some file".getBytes();
        final InputStream file = new ByteArrayInputStream(data);
        final long size = storage().upload(file, TOPIC_PARTITION_SEGMENT_KEY);
        assertThat(size).isEqualTo(data.length);

        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("some file");
        }

        final BytesRange range = BytesRange.of(1, data.length - 2);
        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, range)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fil");
        }

        storage().delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key topic/partition/log does not exists in storage " + storage().toString());
    }

    @Test
    void testUploadANewFile() throws StorageBackendException, IOException {
        final String content = "content";
        uploadContentAsFileAndVerify(content);
    }

    protected void uploadContentAsFileAndVerify(final String content) throws StorageBackendException, IOException {
        final ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());
        final long size = storage().upload(in, TOPIC_PARTITION_SEGMENT_KEY);
        assertThat(size).isEqualTo(content.length());

        assertThat(in).isEmpty();
        in.close();
        assertThat(storage().fetch(TOPIC_PARTITION_SEGMENT_KEY)).hasContent(content);
    }

    @Test
    void testRetryUploadKeepLatestVersion() throws StorageBackendException {
        final String content = "content";
        storage().upload(new ByteArrayInputStream((content + "v1").getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        storage().upload(new ByteArrayInputStream((content + "v2").getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(storage().fetch(TOPIC_PARTITION_SEGMENT_KEY)).hasContent(content + "v2");
    }

    @Test
    void testFetchFailWhenNonExistingKey() {
        assertThatThrownBy(() -> storage().fetch(new TestObjectKey("non-existing")))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage());
        assertThatThrownBy(() -> storage().fetch(new TestObjectKey("non-existing"), BytesRange.of(0, 1)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage());
    }

    @Test
    void testFetchAll() throws IOException, StorageBackendException {
        final String content = "content";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            assertThat(fetch).hasContent(content);
        }
    }

    @Test
    void testFetchWithOffsetRange() throws IOException, StorageBackendException {
        final String content = "AABBBBAA";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        final int from = 2;
        final int to = 5;
        // Replacing end position as substring is end exclusive, and expected response is end inclusive
        final String range = content.substring(from, to + 1);

        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(from, to))) {
            assertThat(fetch).hasContent(range);
        }
    }

    @Test
    void testFetchSingleByte() throws IOException, StorageBackendException {
        final String content = "ABC";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(2, 2))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException, StorageBackendException {
        final String content = "ABC";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(2, 4))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    protected void testFetchWithRangeOutsideFileSize() throws StorageBackendException {
        final String content = "ABC";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(3, 5)))
            .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(4, 6)))
            .isInstanceOf(InvalidRangeException.class);
    }

    @Test
    void testFetchNonExistingKey() {
        assertThatThrownBy(() -> storage().fetch(new TestObjectKey("non-existing")))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage());
        assertThatThrownBy(() -> storage().fetch(new TestObjectKey("non-existing"), BytesRange.of(0, 1)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage());
    }

    @Test
    void testFetchNonExistingKeyMasking() {
        final ObjectKey key = new ObjectKey() {
            @Override
            public String value() {
                return "real-key";
            }

            @Override
            public String toString() {
                return "masked-key";
            }
        };

        assertThatThrownBy(() -> storage().fetch(key))
            .extracting(Throwables::getStackTrace)
            .asString()
            .contains("masked-key")
            .doesNotContain("real-key");

        assertThatThrownBy(() -> storage().fetch(key, BytesRange.of(0, 1)))
            .extracting(Throwables::getStackTrace)
            .asString()
            .contains("masked-key")
            .doesNotContain("real-key");
    }

    @Test
    protected void testDelete() throws StorageBackendException {
        storage().upload(new ByteArrayInputStream("test".getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        storage().delete(TOPIC_PARTITION_SEGMENT_KEY);

        // Test deletion idempotence.
        assertThatNoException().isThrownBy(() -> storage().delete(TOPIC_PARTITION_SEGMENT_KEY));

        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key topic/partition/log does not exists in storage " + storage());
    }

    @Test
    protected void testDeletes() throws StorageBackendException {
        final Set<ObjectKey> keys = IntStream.range(0, 10)
            .mapToObj(i -> new TestObjectKey(TOPIC_PARTITION_SEGMENT_KEY.value() + i))
            .collect(Collectors.toSet());
        for (final var key : keys) {
            storage().upload(new ByteArrayInputStream("test".getBytes()), key);
        }
        storage().delete(keys);

        // Test deletion idempotence.
        assertThatNoException().isThrownBy(() -> storage().delete(keys));

        for (final var key : keys) {
            assertThatThrownBy(() -> storage().fetch(key))
                .isInstanceOf(KeyNotFoundException.class)
                .hasMessage("Key " + key.value() + " does not exists in storage " + storage());
        }
    }
}
