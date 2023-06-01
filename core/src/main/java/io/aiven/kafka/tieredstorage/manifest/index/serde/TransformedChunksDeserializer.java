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

package io.aiven.kafka.tieredstorage.manifest.index.serde;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.github.luben.zstd.Zstd;

public class TransformedChunksDeserializer extends JsonDeserializer<List<Integer>> {
    /**
     * Max size of an encoded transformed chunks we want to serialize.
     * This is mostly for sanity check, it should never be this big.
     */
    static final int MAX_DECOMPRESSED_SIZE = 10 * 1024 * 1024;

    @Override
    public List<Integer> deserialize(final JsonParser p,
                                     final DeserializationContext ctxt) throws IOException {
        final String base64String = p.getValueAsString();
        final byte[] compressed = Base64.getDecoder().decode(base64String);

        final long decompressedSize = Zstd.decompressedSize(compressed);
        if (decompressedSize < 0 || decompressedSize > MAX_DECOMPRESSED_SIZE) {
            throw new IOException("Invalid decompressed size: " + decompressedSize);
        }
        final byte[] binEncoded = new byte[(int) decompressedSize];
        Zstd.decompress(binEncoded, compressed);

        return ChunkSizesBinaryCodec.decode(binEncoded);
    }
}
