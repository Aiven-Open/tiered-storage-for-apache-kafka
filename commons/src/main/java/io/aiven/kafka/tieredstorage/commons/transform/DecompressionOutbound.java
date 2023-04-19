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

import java.util.Objects;

import com.github.luben.zstd.Zstd;

/**
 * The chunk de-transformation that does Zstd decompression.
 */
public class DecompressionOutbound implements OutboundTransform {
    private final OutboundTransform inner;

    DecompressionOutbound(final OutboundTransform inner) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public byte[] nextElement() {
        final byte[] chunk = inner.nextElement();
        final long decompressedSize = Zstd.decompressedSize(chunk);
        if (decompressedSize < 0) {
            throw new RuntimeException("Invalid decompressed size: " + decompressedSize);
        }
        return Zstd.decompress(chunk, (int) decompressedSize);
    }
}
