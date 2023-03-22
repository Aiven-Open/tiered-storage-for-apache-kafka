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

import java.util.Objects;

import com.github.luben.zstd.ZstdCompressCtx;

/**
 * The chunk transformation that does Zstd compression.
 */
public class CompressionChunkEnumeration implements TransformChunkEnumeration {
    private final TransformChunkEnumeration inner;

    public CompressionChunkEnumeration(final TransformChunkEnumeration inner) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
    }

    @Override
    public int originalChunkSize() {
        return inner.originalChunkSize();
    }

    @Override
    public Integer transformedChunkSize() {
        // Variable transformed chunk size.
        return null;
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public byte[] nextElement() {
        final var chunk = inner.nextElement();
        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            try {
                compressCtx.setPledgedSrcSize(chunk.length);
            } catch (final NoSuchMethodError e) {
                // This may happen if the Zstd library is old enough.
                // It's OK if we just skip here, because the operation is advisory.
            }
            // Allow the de-transformation to know the content size and allocate buffers accordingly.
            compressCtx.setContentSize(true);
            return compressCtx.compress(chunk);
        }
    }
}
