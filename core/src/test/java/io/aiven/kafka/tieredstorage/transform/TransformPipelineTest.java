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
import java.util.List;
import java.util.Random;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformPipelineTest extends RsaKeyAwareTest {
    static final int ORIGINAL_SIZE = 1812004;

    static byte[] original;

    @BeforeAll
    static void init() {
        original = new byte[ORIGINAL_SIZE];
        final Random random = new Random();
        random.nextBytes(original);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 13, 1024, 1024 * 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void plaintext(final int chunkSize) throws IOException {
        test(TransformPipeline.newBuilder().withChunkSize(chunkSize).build());
    }

    @ParameterizedTest
    // Small chunks would make encryption and compression tests very slow, skipping them
    @ValueSource(ints = {1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void encryption(final int chunkSize) throws IOException {
        test(TransformPipeline.newBuilder()
            .withChunkSize(chunkSize)
            .withEncryption(publicKeyPem, privateKeyPem)
            .build());
    }

    @ParameterizedTest
    // Small chunks would make compression tests going very slowly, skipping them
    @ValueSource(ints = {1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void compression(final int chunkSize) throws IOException {
        test(TransformPipeline.newBuilder()
            .withChunkSize(chunkSize)
            .withCompression()
            .build());
    }

    @ParameterizedTest
    // Small chunks would make compression tests going very slowly, skipping them
    @ValueSource(ints = {1024 - 1, 1024, 1024 * 2 + 2, 1024 * 5 + 3, ORIGINAL_SIZE - 1, ORIGINAL_SIZE * 2})
    void compressionAndEncryption(final int chunkSize) throws IOException {
        test(TransformPipeline.newBuilder()
            .withChunkSize(chunkSize)
            .withCompression()
            .withEncryption(publicKeyPem, privateKeyPem)
            .build());
    }

    private void test(final TransformPipeline pipeline) throws IOException {
        // Transform.
        final ByteArrayInputStream inputContent = new ByteArrayInputStream(original);
        final InboundTransformChain inboundTransformChain = pipeline.inboundTransformChain(inputContent, ORIGINAL_SIZE);
        final TransformFinisher transformFinisher = inboundTransformChain.complete();

        final byte[] uploadedData;
        try (final var sis = transformFinisher.toInputStream()) {
            uploadedData = sis.readAllBytes();
        }

        // Detransform.
        final SegmentManifest segmentManifest = pipeline.segmentManifest(transformFinisher.chunkIndex());
        final ByteArrayInputStream uploadedContent = new ByteArrayInputStream(uploadedData);
        final List<Chunk> chunks = segmentManifest.chunkIndex().chunks();
        final OutboundTransformChain outboundTransformChain =
            pipeline.outboundTransformChain(uploadedContent, segmentManifest, chunks);
        final var detransformFinisher = outboundTransformChain.complete();
        try (final var sis = detransformFinisher.toInputStream()) {
            final byte[] downloaded = sis.readAllBytes();
            assertThat(downloaded).isEqualTo(original);
        }
    }
}
