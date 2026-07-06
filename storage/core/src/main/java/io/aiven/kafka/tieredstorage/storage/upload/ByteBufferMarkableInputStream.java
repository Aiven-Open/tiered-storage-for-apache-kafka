/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.upload;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Wraps a {@link ByteBuffer} for access via the {@link InputStream} API.
 * Its implement refers to {@link org.apache.kafka.common.utils.ByteBufferInputStream}
 * but with marking and resetting operations added.
 */
public class ByteBufferMarkableInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public ByteBufferMarkableInputStream(final ByteBuffer buffer) {
        byteBuffer = Objects.requireNonNull(buffer);
    }

    public int read() {
        return !this.byteBuffer.hasRemaining() ? -1 : this.byteBuffer.get() & 255;
    }

    public int read(final byte[] bytes, final int off, int len) {
        if (len == 0) {
            return 0;
        } else if (!this.byteBuffer.hasRemaining()) {
            return -1;
        } else {
            len = Math.min(len, this.byteBuffer.remaining());
            this.byteBuffer.get(bytes, off, len);
            return len;
        }
    }

    public int available() {
        return this.byteBuffer.remaining();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(final int readLimit) {
        byteBuffer.mark();
    }

    @Override
    public void reset() {
        byteBuffer.reset();
    }
}
