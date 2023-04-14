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

package io.aiven.kafka.tieredstorage.commons.io;

import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionKeyProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

public class CryptoIOProviderTest extends RsaKeyAwareTest {

    private static final int BUFFER_SIZE = 8_192;

    private static final int MESSAGE_AMOUNT = 1_000;

    private CryptoIOProvider cryptoIOProvider;

    @BeforeEach
    public void setUpKey() throws Exception {
        final EncryptionKeyProvider encProvider = EncryptionKeyProvider.of(
                Files.newInputStream(publicKeyPem),
                Files.newInputStream(privateKeyPem)
        );
        final var key = encProvider.createKey();
        final byte[] encryptionKey = new byte[32];
        System.arraycopy(key.getEncoded(), 0, encryptionKey, 0, 32);
        final byte[] aad = new byte[32];
        System.arraycopy(key.getEncoded(), 32, encryptionKey, 0, 32);
        cryptoIOProvider = new CryptoIOProvider(new SecretKeySpec(encryptionKey, "AES"), aad, BUFFER_SIZE);
    }

    @Test
    public void compressAndEncryptStream(@TempDir final Path tmpFolder) throws Exception {

        final var random = new Random();

        final var testFile = tmpFolder.resolve("original_file");
        final var encryptedFile = tmpFolder.resolve("encrypted_file");
        final var decryptedFile = tmpFolder.resolve("decrypted_file");

        final var message = new byte[BUFFER_SIZE];

        final var expectedBytes = ByteBuffer.allocate(BUFFER_SIZE * MESSAGE_AMOUNT);
        try (final var fin = Files.newOutputStream(testFile)) {
            for (int i = 0; i < MESSAGE_AMOUNT; i++) {
                random.nextBytes(message);
                fin.write(message);
                expectedBytes.put(message);
            }
            fin.flush();
        }

        try (final var in = Files.newInputStream(testFile);
             final var encryptedFileOut = Files.newOutputStream(encryptedFile)) {
            cryptoIOProvider.compressAndEncrypt(in, encryptedFileOut);
        }

        try (final var encryptedFileStream = Files.newInputStream(encryptedFile);
             final var out = Files.newOutputStream(decryptedFile)) {
            IOUtils.copy(cryptoIOProvider.decryptAndDecompress(encryptedFileStream), out, 8_192);
        }

        final var decryptedBytes = ByteBuffer.allocate(BUFFER_SIZE * MESSAGE_AMOUNT);
        try (final var in = Files.newInputStream(decryptedFile)) {
            final var buffer = new byte[8192];
            while (in.read(buffer) != -1) {
                decryptedBytes.put(buffer);
            }
        }

        assertThat(decryptedBytes.array()).isEqualTo(expectedBytes.array());
    }

}
