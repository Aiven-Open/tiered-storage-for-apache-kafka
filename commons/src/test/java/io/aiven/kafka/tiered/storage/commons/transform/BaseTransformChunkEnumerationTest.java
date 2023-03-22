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

package io.aiven.kafka.tiered.storage.commons.transform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BaseTransformChunkEnumerationTest {
    @Test
    void nullInputStream() {
        assertThatThrownBy(() -> new BaseTransformChunkEnumeration(null, 10))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inputStream cannot be null");
    }

    @Test
    void negativeOriginalChunkSize() {
        assertThatThrownBy(() -> new BaseTransformChunkEnumeration(new ByteArrayInputStream(new byte[1]), -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalChunkSize must be non-negative, -1 given");
    }

    @Test
    void originalChunkSize() {
        final var transform = new BaseTransformChunkEnumeration(new ByteArrayInputStream(new byte[1]), 123);
        assertThat(transform.originalChunkSize()).isEqualTo(123);
    }

    @Test
    void transformedChunkSizeEqualsToOriginal() {
        final var transform = new BaseTransformChunkEnumeration(new ByteArrayInputStream(new byte[1]), 123);
        assertThat(transform.transformedChunkSize()).isEqualTo(transform.originalChunkSize());
    }

    @Test
    void emptyInputStream() {
        final var transform = new BaseTransformChunkEnumeration(InputStream.nullInputStream(), 123);
        assertThat(transform.hasMoreElements()).isFalse();
        assertThatThrownBy(transform::nextElement)
            .isInstanceOf(NoSuchElementException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100})
    void inOneChunk(final int originalChunkSize) {
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final var transform = new BaseTransformChunkEnumeration(new ByteArrayInputStream(data), originalChunkSize);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(data);

        assertThat(transform.hasMoreElements()).isFalse();
        assertThatThrownBy(transform::nextElement)
            .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void inManyChunks() {
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final var transform = new BaseTransformChunkEnumeration(new ByteArrayInputStream(data), 3);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {0, 1, 2});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {3, 4, 5});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {6, 7, 8});
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.nextElement()).isEqualTo(new byte[] {9});

        assertThat(transform.hasMoreElements()).isFalse();
        assertThatThrownBy(transform::nextElement)
            .isInstanceOf(NoSuchElementException.class);
    }
}
