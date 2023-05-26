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

package io.aiven.kafka.tieredstorage.core.manifest.index.serde;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.github.luben.zstd.ZstdCompressCtx;

public class TransformedChunksSerializer extends JsonSerializer<List<Integer>> {
    @Override
    public void serialize(final List<Integer> value,
                          final JsonGenerator gen,
                          final SerializerProvider serializers) throws IOException {
        final byte[] binEncoded = ChunkSizesBinaryCodec.encode(value);

        if (binEncoded.length > TransformedChunksDeserializer.MAX_DECOMPRESSED_SIZE) {
            throw new IllegalArgumentException(
                "Encoded index is too big (" + binEncoded.length + "), cannot serialize");
        }

        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            try {
                compressCtx.setPledgedSrcSize(binEncoded.length);
            } catch (final NoSuchMethodError e) {
                // This may happen if the Zstd library is old enough.
                // It's OK if we just skip here, because the operation is advisory.
            }
            compressCtx.setContentSize(true);
            final byte[] compressed = compressCtx.compress(binEncoded);
            final String base64String = Base64.getEncoder().encodeToString(compressed);
            gen.writeString(base64String);
        }
    }
}
