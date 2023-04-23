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

package io.aiven.kafka.tieredstorage.commons.transform;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.commons.Chunk;

/**
 * The base chunk de-transformation that does the initial chunking of the input stream of bytes.
 *
 * <p>An important quality of this class is that when we use it we know already the chunk positions and sizes,
 * both transformed and not. We rely on this information for determining the transformed chunks borders
 * in the input stream. We also can tell if the input stream has too few bytes.
 */
public class BaseDetransformChunkEnumeration implements DetransformChunkEnumeration {
    private final InputStream inputStream;
    private final Iterator<Chunk> chunksIter;

    private byte[] chunk = null;

    public BaseDetransformChunkEnumeration(final InputStream inputStream,
                                           final Chunk chunk) {
        this(inputStream, List.of(Objects.requireNonNull(chunk, "chunk cannot be null")));
    }

    public BaseDetransformChunkEnumeration(final InputStream inputStream,
                                           final List<Chunk> chunks) {
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream cannot be null");
        this.chunksIter = Objects.requireNonNull(chunks, "chunks cannot be null").iterator();
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

        if (!chunksIter.hasNext()) {
            chunk = new byte[0];
            return;
        }

        try {
            final int expectedTransformedSize = chunksIter.next().transformedSize;
            chunk = inputStream.readNBytes(expectedTransformedSize);
            if (chunk.length < expectedTransformedSize) {
                throw new RuntimeException("Stream has fewer bytes than expected");
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean endOfStreamReached() {
        return chunk.length == 0;
    }
}
