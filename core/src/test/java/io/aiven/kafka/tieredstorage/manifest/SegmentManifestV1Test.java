/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.manifest;

import javax.crypto.spec.SecretKeySpec;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SegmentManifestV1Test {
    @Test
    void identical() {
        final byte[] keyBytes = new byte[32];
        final byte[] aad = new byte[32];
        final var i1 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes, "AES"), aad);
        final var i2 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes, "AES"), aad);
        assertThat(i1).isEqualTo(i2);
        assertThat(i2).isEqualTo(i1);
        assertThat(i1).hasSameHashCodeAs(i2);
    }

    @Test
    void differentDataKey() {
        final byte[] keyBytes1 = new byte[32];
        final byte[] keyBytes2 = new byte[32];
        Arrays.fill(keyBytes2, (byte) 1);
        final byte[] aad = new byte[32];
        final var i1 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes1, "AES"), aad);
        final var i2 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes2, "AES"), aad);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }

    @Test
    void differentAAD() {
        final byte[] keyBytes = new byte[32];
        final byte[] aad1 = new byte[32];
        final byte[] aad2 = new byte[32];
        Arrays.fill(aad2, (byte) 1);
        final var i1 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes, "AES"), aad1);
        final var i2 = new SegmentEncryptionMetadataV1(new SecretKeySpec(keyBytes, "AES"), aad2);
        assertThat(i1).isNotEqualTo(i2);
        assertThat(i2).isNotEqualTo(i1);
        assertThat(i1).doesNotHaveSameHashCodeAs(i2);
    }
}
