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

package io.aiven.kafka.tieredstorage.iceberg.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Reads the batches of a Kafka log segment file (message format v2).
 * A partial batch at the end of the file is ignored, as Kafka does.
 */
public final class KafkaLogReader implements Iterator<KafkaBatch>, Closeable {
    private static final int LOG_OVERHEAD = 12;
    private static final int MIN_BATCH_SIZE = 14;

    private final FileChannel channel;
    private long position = 0;
    private KafkaBatch next;

    private KafkaLogReader(final FileChannel channel) {
        this.channel = channel;
    }

    public static KafkaLogReader open(final Path logFile) throws IOException {
        return new KafkaLogReader(FileChannel.open(logFile, StandardOpenOption.READ));
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                next = readNext();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return next != null;
    }

    @Override
    public KafkaBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KafkaBatch result = next;
        next = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private KafkaBatch readNext() throws IOException {
        final long end = channel.size();
        if (position + LOG_OVERHEAD > end) {
            return null;
        }
        final ByteBuffer overhead = ByteBuffer.allocate(LOG_OVERHEAD);
        readFully(overhead, position);
        final int length = overhead.getInt(8);
        if (length < MIN_BATCH_SIZE) {
            throw new IllegalStateException("Invalid batch size " + length + " at position " + position);
        }
        if (position + LOG_OVERHEAD + length > end) {
            return null;
        }
        final ByteBuffer batch = ByteBuffer.allocate(LOG_OVERHEAD + length);
        readFully(batch, position);
        position += batch.capacity();
        return KafkaBatchParser.parse(batch.flip());
    }

    private void readFully(final ByteBuffer buffer, final long from) throws IOException {
        long at = from;
        while (buffer.hasRemaining()) {
            final int read = channel.read(buffer, at);
            if (read < 0) {
                throw new IOException("Unexpected end of file");
            }
            at += read;
        }
    }
}
