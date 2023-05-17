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

public abstract class BaseStorageTest {

    protected static final String TOPIC_PARTITION_SEGMENT_KEY = "topic/partition/log";

    protected abstract FileUploader uploader();

    protected abstract FileFetcher fetcher();

    protected abstract FileDeleter deleter();


    @Test
    void testUploadFetchDelete() throws IOException, StorageBackEndException {
        final byte[] data = "some file".getBytes();
        final InputStream file = new ByteArrayInputStream(data);
        uploader().upload(file, TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("some file");
        }

        final BytesRange range = BytesRange.of(1, data.length - 2);
        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, range)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fil");
        }

        deleter().delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key topic/partition/log does not exists in storage " + fetcher().toString());
    }

    @Test
    void testUploadANewFile() throws StorageBackEndException {
        final String content = "content";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY)).hasContent(content);
    }

    @Test
    void testRetryUploadKeepLatestVersion() throws StorageBackEndException {
        final String content = "content";
        uploader().upload(new ByteArrayInputStream((content + "v1").getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        uploader().upload(new ByteArrayInputStream((content + "v2").getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThat(fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY)).hasContent(content + "v2");
    }

    @Test
    void testFetchFailWhenNonExistingKey() {
        assertThatThrownBy(() -> fetcher().fetch("non-existing"))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + fetcher());
        assertThatThrownBy(() -> fetcher().fetch("non-existing", BytesRange.of(0, 1)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + fetcher());
    }

    @Test
    void testFetchAll() throws IOException, StorageBackEndException {
        final String content = "content";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY)) {
            assertThat(fetch).hasContent(content);
        }
    }

    @Test
    void testFetchWithOffsetRange() throws IOException, StorageBackEndException {
        final String content = "AABBBBAA";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        final int from = 2;
        final int to = 5;
        // Replacing end position as substring is end exclusive, and expected response is end inclusive
        final String range = content.substring(from, to + 1);

        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(from, to))) {
            assertThat(fetch).hasContent(range);
        }
    }

    @Test
    void testFetchSingleByte() throws IOException, StorageBackEndException {
        final String content = "ABC";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(2, 2))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException, StorageBackEndException {
        final String content = "ABC";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        try (final InputStream fetch = fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(2, 4))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    void testFetchWithRangeOutsideFileSize() throws StorageBackEndException {
        final String content = "ABC";
        uploader().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(3, 5)))
            .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(4, 6)))
            .isInstanceOf(InvalidRangeException.class);
    }

    @Test
    void testFetchNonExistingKey() {
        assertThatThrownBy(() -> fetcher().fetch("non-existing"))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + fetcher());
        assertThatThrownBy(() -> fetcher().fetch("non-existing", BytesRange.of(0, 1)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + fetcher());
    }
    
    @Test
    protected void testDelete() throws StorageBackEndException {
        uploader().upload(new ByteArrayInputStream("test".getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        deleter().delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> fetcher().fetch(TOPIC_PARTITION_SEGMENT_KEY))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key topic/partition/log does not exists in storage " + fetcher());
    }
}
