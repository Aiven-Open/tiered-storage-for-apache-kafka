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

package io.aiven.kafka.tieredstorage.commons.security;

import javax.crypto.spec.SecretKeySpec;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EncryptionKeyAndAADEqualsTest {
    @Test
    void identical() {
        final byte[] keyBytes = new byte[32];
        final byte[] aad = new byte[32];
        final var ekaad1 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes, "AES"), aad);
        final var ekaad2 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes, "AES"), aad);
        assertThat(ekaad1).isEqualTo(ekaad2);
        assertThat(ekaad2).isEqualTo(ekaad1);
        assertThat(ekaad1).hasSameHashCodeAs(ekaad2);
    }

    @Test
    void differentKeys() {
        final byte[] keyBytes1 = new byte[32];
        final byte[] keyBytes2 = new byte[32];
        Arrays.fill(keyBytes2, (byte) 1);
        final byte[] aad = new byte[32];
        final var ekaad1 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes1, "AES"), aad);
        final var ekaad2 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes2, "AES"), aad);
        assertThat(ekaad1).isNotEqualTo(ekaad2);
        assertThat(ekaad2).isNotEqualTo(ekaad1);
        assertThat(ekaad1).doesNotHaveSameHashCodeAs(ekaad2);
    }

    @Test
    void differentAADs() {
        final byte[] keyBytes = new byte[32];
        final byte[] aad1 = new byte[32];
        final byte[] aad2 = new byte[32];
        Arrays.fill(aad2, (byte) 1);
        final var ekaad1 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes, "AES"), aad1);
        final var ekaad2 = new SecretKeyAndAAD(new SecretKeySpec(keyBytes, "AES"), aad2);
        assertThat(ekaad1).isNotEqualTo(ekaad2);
        assertThat(ekaad2).isNotEqualTo(ekaad1);
        assertThat(ekaad1).doesNotHaveSameHashCodeAs(ekaad2);
    }
}
