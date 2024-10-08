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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

import io.aiven.kafka.tieredstorage.AesKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformsEndToEndTest extends AesKeyAwareTest {
    static final int ORIGINAL_SIZE = 1812004;

    static byte[] original;

    @BeforeAll
    static void init() {
        original = new byte[ORIGINAL_SIZE];
        final var random = new Random();
        random.nextBytes(original);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 5, 13, 1024, 1024 * 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void plaintext(final int chunkSize) throws IOException {
        test(chunkSize, false, false);
    }

    @ParameterizedTest
    // Small chunks would make encryption and compression tests very slow, skipping them
    @ValueSource(ints = {0, 1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void encryption(final int chunkSize) throws IOException {
        test(chunkSize, false, true);
    }

    @ParameterizedTest
    // Small chunks would make compression tests going very slowly, skipping them
    @ValueSource(ints = {1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void compression(final int chunkSize) throws IOException {
        test(chunkSize, true, false);
    }

    @ParameterizedTest
    // Small chunks would make compression tests going very slowly, skipping them
    @ValueSource(ints = {1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void compressionAndEncryption(final int chunkSize) throws IOException {
        test(chunkSize, true, true);
    }

    private void test(final int chunkSize, final boolean compression, final boolean encryption) throws IOException {
        try (final var inputStream = new ByteArrayInputStream(original)) {
            // Transform.
            TransformChunkEnumeration transformEnum = new BaseTransformChunkEnumeration(inputStream, chunkSize);
            if (compression) {
                transformEnum = new CompressionChunkEnumeration(transformEnum);
            }
            if (encryption) {
                transformEnum = new EncryptionChunkEnumeration(
                    transformEnum,
                    AesKeyAwareTest::encryptionCipherSupplier
                );
            }
            final var transformBuilder = TransformFinisher.newBuilder(transformEnum, ORIGINAL_SIZE);
            if (chunkSize == 0) {
                transformBuilder.withChunkingDisabled();
            }
            final var transformFinisher = transformBuilder.build();
            final byte[] uploadedData;
            final ChunkIndex chunkIndex;
            try (final var sis = transformFinisher.toInputStream()) {
                uploadedData = sis.readAllBytes();
                chunkIndex = transformFinisher.chunkIndex();
            }

            // Detransform.
            try (final var uploadedStream = new ByteArrayInputStream(uploadedData)) {
                DetransformChunkEnumeration detransformEnum = new BaseDetransformChunkEnumeration(
                    uploadedStream,
                    chunkIndex.chunks()
                );
                if (encryption) {
                    detransformEnum = new DecryptionChunkEnumeration(
                        detransformEnum, ivSize, AesKeyAwareTest::decryptionCipherSupplier);
                }
                if (compression) {
                    detransformEnum = new DecompressionChunkEnumeration(detransformEnum);
                }
                final var detransformFinisher = new DetransformFinisher(detransformEnum);
                try (final var sis = detransformFinisher.toInputStream()) {
                    final byte[] downloaded = sis.readAllBytes();
                    assertThat(downloaded).isEqualTo(original);
                }
            }
        }
    }
}
