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

package io.aiven.kafka.tieredstorage.core.manifest.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import io.aiven.kafka.tieredstorage.core.Chunk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.github.luben.zstd.ZstdCompressCtx;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ChunkIndexSerializationTest {
    final ObjectMapper mapper = new ObjectMapper();

    static final String ENCODED_CHUNKS = "KLUv/SAPeQAAAAAAAwAAAAoBAAoAAAAe";

    // This should give an idea what's inside ENCODED_CHUNKS.
    static {
        // Three values are encoded: 10, 20, 30.
        final ByteBuffer buf = ByteBuffer.allocate(100);
        buf.putInt(3);  // count
        buf.putInt(10);  // base
        buf.put((byte) 1);  // bytes per value
        buf.put((byte) 0);  // i.e. 10
        buf.put((byte) 10);  // i.e. 20
        buf.putInt(30);  // i.e. 30 (no base)
        buf.limit(buf.position());
        buf.rewind();

        final byte[] compressed;
        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            compressCtx.setContentSize(true);
            compressed = compressCtx.compress(Arrays.copyOfRange(buf.array(), 0, buf.limit()));
        }
        final String expectedEncodedValue = Base64.getEncoder().encodeToString(compressed);
        assertThat(expectedEncodedValue).isEqualTo(ENCODED_CHUNKS);
    }

    static final String FIXED_SIZE_CHUNK_INDEX_JSON = "{"
        + "\"type\":\"fixed\","
        + "\"originalChunkSize\":100,"
        + "\"originalFileSize\":250,"
        + "\"transformedChunkSize\":110,"
        + "\"finalTransformedChunkSize\":30}";

    static final String VARIABLE_CHUNK_INDEX_JSON = "{"
        + "\"type\":\"variable\","
        + "\"originalChunkSize\":100,"
        + "\"originalFileSize\":250,"
        + "\"transformedChunks\":\"" + ENCODED_CHUNKS + "\"}";

    @Test
    void serializeFixedSizeChunkIndex() throws JsonProcessingException {
        final var fixedSizeChunkIndex = new FixedSizeChunkIndex(
            100, 250, 110, 30);
        assertThat(mapper.writeValueAsString(fixedSizeChunkIndex))
            .isEqualTo(FIXED_SIZE_CHUNK_INDEX_JSON);
    }

    @Test
    void deserializeFixedSizeChunkIndex() throws IOException, NoSuchFieldException, IllegalAccessException {
        final FixedSizeChunkIndex index = mapper.readValue(FIXED_SIZE_CHUNK_INDEX_JSON, FixedSizeChunkIndex.class);

        assertThat(index.originalChunkSize).isEqualTo(100);
        assertThat(index.originalFileSize).isEqualTo(250);
        assertThat(index.finalTransformedChunkSize).isEqualTo(30);
        assertThat(index.transformedChunkSize).isEqualTo(110);

        assertThat(index.chunks()).containsExactly(
            new Chunk(0, 0, 100, 0, 110),
            new Chunk(1, 100, 100, 110, 110),
            new Chunk(2, 200, 50, 220, 30)
        );
        assertThat(index.chunkCount).isEqualTo(3);
    }

    @Test
    void serializeVariableSizeChunkIndex() throws JsonProcessingException {
        final var variableSizeChunkIndex = new VariableSizeChunkIndex(
            100, 250, List.of(10, 20, 30));

        assertThat(mapper.writeValueAsString(variableSizeChunkIndex))
            .isEqualTo(VARIABLE_CHUNK_INDEX_JSON);
    }

    @Test
    void deserializeVariableSizeChunkIndex() throws JsonProcessingException {
        final VariableSizeChunkIndex index = mapper.readValue(VARIABLE_CHUNK_INDEX_JSON, VariableSizeChunkIndex.class);

        assertThat(index.originalChunkSize).isEqualTo(100);
        assertThat(index.originalFileSize).isEqualTo(250);

        assertThat(index.chunks()).containsExactly(
            new Chunk(0, 0, 100, 0, 10),
            new Chunk(1, 100, 100, 10, 20),
            new Chunk(2, 200, 50, 30, 30)
        );
        assertThat(index.chunkCount).isEqualTo(3);
    }

    @Test
    void deserializationShouldRequireFieldsForFixedSizeChunkIndex() {
        final String json1 = "{"
            + "\"type\":\"fixed\","
            + "\"originalFileSize\":250,"
            + "\"transformedChunkSize\":110,"
            + "\"finalTransformedChunkSize\":30}";
        assertThatThrownBy(() -> mapper.readValue(json1, FixedSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'originalChunkSize' (index 0)");

        final String json2 = "{"
            + "\"type\":\"fixed\","
            + "\"originalChunkSize\":100,"
            + "\"transformedChunkSize\":110,"
            + "\"finalTransformedChunkSize\":30}";
        assertThatThrownBy(() -> mapper.readValue(json2, FixedSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'originalFileSize' (index 1)");

        final String json3 = "{"
            + "\"type\":\"fixed\","
            + "\"originalChunkSize\":100,"
            + "\"originalFileSize\":250,"
            + "\"finalTransformedChunkSize\":30}";
        assertThatThrownBy(() -> mapper.readValue(json3, FixedSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'transformedChunkSize' (index 2)");

        final String json4 = "{"
            + "\"type\":\"fixed\","
            + "\"originalChunkSize\":100,"
            + "\"originalFileSize\":250,"
            + "\"transformedChunkSize\":110}";
        assertThatThrownBy(() -> mapper.readValue(json4, FixedSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'finalTransformedChunkSize' (index 3)");
    }

    @Test
    void deserializationShouldRequireFieldsForVariableSizeChunkIndex() {
        final String json1 = "{"
            + "\"type\":\"variable\","
            + "\"originalFileSize\":250,"
            + "\"transformedChunks\":\"" + ENCODED_CHUNKS + "\"}";
        assertThatThrownBy(() -> mapper.readValue(json1, VariableSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'originalChunkSize' (index 0)");

        final String json2 = "{"
            + "\"type\":\"variable\","
            + "\"originalChunkSize\":100,"
            + "\"transformedChunks\":\"" + ENCODED_CHUNKS + "\"}";
        assertThatThrownBy(() -> mapper.readValue(json2, VariableSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'originalFileSize' (index 1)");

        final String json3 = "{"
            + "\"type\":\"variable\","
            + "\"originalChunkSize\":100,"
            + "\"originalFileSize\":250}";
        assertThatThrownBy(() -> mapper.readValue(json3, VariableSizeChunkIndex.class))
            .isInstanceOf(MismatchedInputException.class)
            .hasMessageStartingWith("Missing required creator property 'transformedChunks' (index 2)");
    }

    @Test
    void deserializeToInterface() throws JsonProcessingException {
        assertThat(mapper.readValue(FIXED_SIZE_CHUNK_INDEX_JSON, ChunkIndex.class))
            .isInstanceOf(FixedSizeChunkIndex.class);
        assertThat(mapper.readValue(VARIABLE_CHUNK_INDEX_JSON, ChunkIndex.class))
            .isInstanceOf(VariableSizeChunkIndex.class);
    }
}
