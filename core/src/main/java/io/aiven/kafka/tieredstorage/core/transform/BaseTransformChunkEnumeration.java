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

package io.aiven.kafka.tieredstorage.core.transform;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * The base chunk transformation that does the initial chunking of the input stream of bytes.
 */
public class BaseTransformChunkEnumeration implements TransformChunkEnumeration {
    private final InputStream inputStream;
    private final int originalChunkSize;

    private byte[] chunk = null;

    public BaseTransformChunkEnumeration(final InputStream inputStream,
                                         final int originalChunkSize) {
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream cannot be null");

        if (originalChunkSize < 0) {
            throw new IllegalArgumentException(
                "originalChunkSize must be non-negative, " + originalChunkSize + " given");
        }
        this.originalChunkSize = originalChunkSize;
    }

    @Override
    public int originalChunkSize() {
        return originalChunkSize;
    }

    @Override
    public Integer transformedChunkSize() {
        // No real transformation done, no size changes.
        return originalChunkSize;
    }

    @Override
    public boolean hasMoreElements() {
        fillChunkIfNeeded();
        return !endOfStreamReached();
    }

    @Override
    public byte[] nextElement() {
        fillChunkIfNeeded();
        if (endOfStreamReached()) {
            throw new NoSuchElementException();
        }

        final var result = chunk;
        chunk = null;
        return result;
    }

    private void fillChunkIfNeeded() {
        if (chunk != null) {
            return;
        }

        try {
            chunk = inputStream.readNBytes(originalChunkSize);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean endOfStreamReached() {
        return chunk.length == 0;
    }
}
