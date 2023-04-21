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

import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EncryptionProviderTest extends RsaKeyAwareTest {

    @Test
    void alwaysGeneratesNewKey() throws IOException, NoSuchAlgorithmException, NoSuchProviderException {
        final var rsaProvider =
                RsaEncryptionProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );

        final AesEncryptionProvider aesProvider = new AesEncryptionProvider(rsaProvider.keyGenerator());
        final var dataKey1 = aesProvider.createDataKey();
        final var dataKey2 = aesProvider.createDataKey();

        assertThat(dataKey1).isNotEqualTo(dataKey2);
    }

    @Test
    void decryptGeneratedKey() throws IOException, NoSuchAlgorithmException, NoSuchProviderException {
        final var rsaEncryptionProvider =
                RsaEncryptionProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        final AesEncryptionProvider aesProvider = new AesEncryptionProvider(rsaEncryptionProvider.keyGenerator());
        final var dataKey = aesProvider.createDataKey();
        final var encryptedKey = rsaEncryptionProvider.encryptKey(dataKey);
        final var restoredKey = rsaEncryptionProvider.decryptKey(encryptedKey);

        assertThat(new SecretKeySpec(restoredKey, "AES")).isEqualTo(dataKey);
    }

}
