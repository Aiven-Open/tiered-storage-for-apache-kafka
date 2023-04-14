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

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EncryptionKeyProviderTest extends RsaKeyAwareTest {

    @Test
    void alwaysGeneratesNewKey() throws IOException {
        final var ekp =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );

        final var key1 = ekp.createKey();
        final var key2 = ekp.createKey();

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void decryptGeneratedKey() throws IOException {
        final var ekProvider =
                EncryptionKeyProvider.of(
                        Files.newInputStream(publicKeyPem),
                        Files.newInputStream(privateKeyPem)
                );
        final var secretKey = ekProvider.createKey();
        final var encryptedKey = ekProvider.encryptKey(secretKey);
        final var restoredKey = ekProvider.decryptKey(encryptedKey);

        assertThat(new SecretKeySpec(restoredKey, "AES")).isEqualTo(secretKey);
    }

}
