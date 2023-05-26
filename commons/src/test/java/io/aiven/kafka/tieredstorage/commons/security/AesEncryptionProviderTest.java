/*
 * Copyright 2021 Aiven Oy
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

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AesEncryptionProviderTest extends RsaKeyAwareTest {

    @Test
    void keyAndAadMustBePresent() {
        final AesEncryptionProvider aesProvider = new AesEncryptionProvider();
        final DataKeyAndAAD dataKeyAndAAD = aesProvider.createDataKeyAndAAD();
        assertThat(dataKeyAndAAD.dataKey.getEncoded()).isNotEmpty();
        assertThat(dataKeyAndAAD.aad).isNotEmpty();
    }

    @Test
    void alwaysGeneratesNewKey() {
        final AesEncryptionProvider aesProvider = new AesEncryptionProvider();
        final DataKeyAndAAD dataKeyAndAad1 = aesProvider.createDataKeyAndAAD();
        final DataKeyAndAAD dataKeyAndAad2 = aesProvider.createDataKeyAndAAD();

        assertThat(dataKeyAndAad1).isNotEqualTo(dataKeyAndAad2);
    }

    @Test
    void keyMustBeDifferentFromAAD() {
        final DataKeyAndAAD dataKeyAndAad = new AesEncryptionProvider().createDataKeyAndAAD();
        assertThat(dataKeyAndAad.dataKey.getEncoded()).isNotEqualTo(dataKeyAndAad.aad);
    }

    @Test
    void decryptGeneratedKey() {
        final var rsaEncryptionProvider = RsaEncryptionProvider.of(publicKeyPem, privateKeyPem);
        final AesEncryptionProvider aesProvider = new AesEncryptionProvider();
        final var dataKey = aesProvider.createDataKeyAndAAD().dataKey;
        final var encryptedKey = rsaEncryptionProvider.encryptDataKey(dataKey);
        final var restoredKey = rsaEncryptionProvider.decryptDataKey(encryptedKey);

        assertThat(new SecretKeySpec(restoredKey, "AES")).isEqualTo(dataKey);
    }

}
