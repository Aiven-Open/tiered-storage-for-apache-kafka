/*
 * Copyright 2024 Aiven Oy
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

import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class SanitizedStorageExceptionHandlerTest {
    static final String RAW_URL = "https://storage.googleapis.com/storage/v1/b/bucket-name/o/"
            + "root_prefix%2Fsub1%2Fsub2%2Ft1-topic_id%2F9%2F00000001-segment_id.log";
    static final String SANITIZED_URL = "[REDACTED]/t1-topic_id/9/00000001-segment_id.log";
    static final Logger LOG = LoggerFactory.getLogger(SanitizedStorageExceptionHandlerTest.class);

    @Test void testSanitizeExceptionWithDirectUrl() {
        final var originalException = new StorageException(503, "503 Service Unavailable DELETE "
            + RAW_URL);
        final var sanitized = SanitizedStorageExceptionHandler.sanitizeException(originalException);
        assertThat(sanitized.getMessage()).isEqualTo("com.google.cloud.storage.StorageException: "
            + "503 Service Unavailable DELETE "
            + SANITIZED_URL);
        LOG.error("test", sanitized);
    }

    @Test
    void testSanitizeExceptionWithNestedEncodedUrl() {
        final var deepestCause = new RuntimeException("Deepest cause: GET " + RAW_URL);
        final var middleCause = new Exception("Middle cause", deepestCause);
        final var topException = new StorageException(500, "Top level exception", middleCause);
        final var sanitized = SanitizedStorageExceptionHandler.sanitizeException(topException);
        assertThat(sanitized.getMessage())
            .startsWith("com.google.cloud.storage.StorageException: Top level exception\n")
            .contains("Caused by: java.lang.Exception: Middle cause\n")
            .contains("Caused by: java.lang.RuntimeException: Deepest cause: GET " + SANITIZED_URL);
        LOG.error("test", sanitized);
    }

    @Test
    void testSanitizeExceptionWithNoUrl() {
        final var exception = new Exception("No URL in this exception");
        final var sanitized = SanitizedStorageExceptionHandler.sanitizeException(exception);
        assertThat(sanitized.getMessage())
            .isEqualTo("java.lang.Exception: No URL in this exception");
        LOG.error("test", sanitized);
    }
}
