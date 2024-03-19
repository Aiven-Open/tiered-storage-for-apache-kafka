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

package io.aiven.kafka.tieredstorage.storage.gcs;

import org.junit.jupiter.api.Test;

import static io.aiven.kafka.tieredstorage.storage.gcs.MetricCollector.OBJECT_DOWNLOAD_PATH_PATTERN;
import static io.aiven.kafka.tieredstorage.storage.gcs.MetricCollector.OBJECT_METADATA_PATH_PATTERN;
import static io.aiven.kafka.tieredstorage.storage.gcs.MetricCollector.OBJECT_UPLOAD_PATH_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

class MetricCollectorRegexTest {

    @Test
    void validBucketName() {
        final var bucketName = "test-bucket_with.all_chars123";
        final var prefix = "tiered-storage-demo";
        final var topic = "topic1";
        final var topicId = "_dCovw9-QaebnUIeIsIJvg";
        final var partition = "0";
        assertThat(OBJECT_METADATA_PATH_PATTERN.matcher(
            "/storage/v1/b/"
                + bucketName
                + "/o/"
                + prefix + "%2F"
                + topic + "-" + topicId + "%2F" + partition
                + "%2F"
                + "00000000000000023511-mhTGflMpQJyceHAOQja1sw.indexes"
        )).matches();
        assertThat(OBJECT_DOWNLOAD_PATH_PATTERN.matcher(
            "/download/storage/v1/b/"
                + bucketName
                + "/o/"
                + prefix + "%2F"
                + topic + "-" + topicId + "%2F" + partition
                + "%2F"
                + "00000000000000023511-mhTGflMpQJyceHAOQja1sw.indexes"
        )).matches();
        assertThat(OBJECT_UPLOAD_PATH_PATTERN.matcher(
            "/upload/storage/v1/b/"
                + bucketName
                + "/o"
        )).matches();
    }

    @Test
    void invalidBucketName() {
        final var bucketName = "test/invalid";
        final var prefix = "tiered-storage-demo";
        final var topic = "topic1";
        final var topicId = "_dCovw9-QaebnUIeIsIJvg";
        final var partition = "0";
        assertThat(OBJECT_METADATA_PATH_PATTERN.matcher(
            "/storage/v1/b/"
                + bucketName
                + "/o/"
                + prefix + "%2F"
                + topic + "-" + topicId + "%2F" + partition
                + "%2F"
                + "00000000000000023511-mhTGflMpQJyceHAOQja1sw.indexes"
        ).matches()).isFalse();
        assertThat(OBJECT_DOWNLOAD_PATH_PATTERN.matcher(
            "/download/storage/v1/b/"
                + bucketName
                + "/o/"
                + prefix + "%2F"
                + topic + "-" + topicId + "%2F" + partition
                + "%2F"
                + "00000000000000023511-mhTGflMpQJyceHAOQja1sw.indexes"
        ).matches()).isFalse();
        assertThat(OBJECT_UPLOAD_PATH_PATTERN.matcher(
            "/upload/storage/v1/b/"
                + bucketName
                + "/o"
        ).matches()).isFalse();
    }
}
