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

package io.aiven.kafka.tieredstorage.transform;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.Chunk;

/**
 * The base chunk de-transformation that does the initial chunking of the input stream of bytes.
 *
 * <p>An important quality of this class is that when we use it we know already the chunk positions and sizes,
 * both transformed and not. We rely on this information for determining the transformed chunks borders
 * in the input stream. We also can tell if the input stream has too few bytes.
 *
 * <p>An empty list of chunks means no chunking has been applied to the incoming stream
 * and all content should be returned at once.
 */
public class BaseDetransformChunkEnumeration implements DetransformChunkEnumeration {
    final InputStream inputStream;
    private boolean inputStreamClosed = false;
    private final Iterator<Chunk> chunksIter;
    private final boolean isEmpty;

    private byte[] chunk = null;

    public BaseDetransformChunkEnumeration(final InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream cannot be null");
        this.isEmpty = true;
        this.chunksIter = Collections.emptyIterator();
    }

    public BaseDetransformChunkEnumeration(final InputStream inputStream,
                                           final List<Chunk> chunks) {
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream cannot be null");
        this.chunksIter = Objects.requireNonNull(chunks, "chunks cannot be null").iterator();
        this.isEmpty = chunks.isEmpty();
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

        if (!chunksIter.hasNext() && !isEmpty) {
            chunk = new byte[0];

            if (!inputStreamClosed) {
                try {
                    inputStream.close();
                    inputStreamClosed = true;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return;
        } else {
            if (inputStreamClosed) {
                throw new RuntimeException("Input stream already closed");
            }
        }

        try {
            if (!isEmpty) {
                final int expectedTransformedSize = chunksIter.next().transformedSize;
                chunk = inputStream.readNBytes(expectedTransformedSize);
                if (chunk.length < expectedTransformedSize) {
                    throw new RuntimeException("Stream has fewer bytes than expected");
                }
            } else {
                chunk = inputStream.readAllBytes();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean endOfStreamReached() {
        return chunk.length == 0;
    }
}
