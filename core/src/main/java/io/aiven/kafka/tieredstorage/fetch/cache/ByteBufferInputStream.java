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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

class ByteBufferInputStream extends InputStream {
    private final RefCountedByteBuffer source;
    private final ByteBuffer buffer;
    private boolean closed;

    ByteBufferInputStream(final RefCountedByteBuffer source) {
        this.source = source;
        this.buffer = source.duplicate();
    }

    @Override
    public int read() {
        if (closed || !buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        if (length == 0) {
            return 0;
        }
        if (closed || !buffer.hasRemaining()) {
            return -1;
        }
        final int bytesToRead = Math.min(length, buffer.remaining());
        buffer.get(bytes, offset, bytesToRead);
        return bytesToRead;
    }

    @Override
    public long skip(final long n) {
        if (closed || n <= 0) {
            return 0;
        }
        final int bytesToSkip = (int) Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + bytesToSkip);
        return bytesToSkip;
    }

    @Override
    public int available() {
        if (closed) {
            return 0;
        }
        return buffer.remaining();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            source.release();
        }
    }
}
