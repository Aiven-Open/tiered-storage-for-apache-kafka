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

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.nio.file.Files;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RsaEncryptionProviderTest extends RsaKeyAwareTest {

    @Test
    void alwaysGeneratesNewKey() throws IOException, NoSuchAlgorithmException, NoSuchProviderException {
        final var ekp =
                RsaEncryptionProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );

        final AesEncryptionProvider aesProvider = new AesEncryptionProvider(ekp.keyGenerator());
        final var key1 = aesProvider.createKey();
        final var key2 = aesProvider.createKey();

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void decryptGeneratedKey() throws IOException, NoSuchAlgorithmException, NoSuchProviderException {
        final var ekProvider =
                RsaEncryptionProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        final AesEncryptionProvider aesProvider = new AesEncryptionProvider(ekProvider.keyGenerator());
        final var secretKey = aesProvider.createKey();
        final var encryptedKey = ekProvider.encryptKey(secretKey);
        final var restoredKey = ekProvider.decryptKey(encryptedKey);

        assertThat(new SecretKeySpec(restoredKey, "AES")).isEqualTo(secretKey);
    }

}
