/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import io.aiven.kafka.tieredstorage.storage.upload.ByteBufferMarkableInputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteBufferMarkableInputStreamTest {

    @Test
    void readAfterAllBytesReadWithoutReset() throws IOException {
        final byte[] buffer = new byte[10];
        new Random().nextBytes(buffer);
        try (ByteBufferMarkableInputStream inputStream = new ByteBufferMarkableInputStream(ByteBuffer.wrap(buffer))){
            assertThat(inputStream.markSupported()).isTrue();
            assertThat(inputStream.available()).isEqualTo(10);
            inputStream.readAllBytes();
            assertThat(inputStream.available()).isEqualTo(0);
            final int read = inputStream.read();
            assertThat(read).isEqualTo(-1);
        }
    }

    @Test
    void readAfterAllBytesReadAndReset() throws IOException {
        final byte[] buffer = new byte[10];
        new Random().nextBytes(buffer);
        try (ByteBufferMarkableInputStream inputStream = new ByteBufferMarkableInputStream(ByteBuffer.wrap(buffer))){
            inputStream.mark(0);
            inputStream.readAllBytes();
            assertThat(inputStream.available()).isEqualTo(0);
            //reset and try to read again
            inputStream.reset();
            assertThat(inputStream.available()).isEqualTo(10);
            final int read = inputStream.read();
            assertThat(read).isNotEqualTo(-1);
        }
    }
}
