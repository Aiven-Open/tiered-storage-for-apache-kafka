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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPairGenerator;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider.RsaKeysReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RsaKeysReaderTest extends RsaKeyAwareTest {

    @Test
    public void failsForUnknownPaths() {
        assertThatThrownBy(
            () -> RsaKeysReader.readRsaKeyPair(Paths.get("."), Paths.get(".")))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsIllegalArgumentExceptionUnsupportedKey(@TempDir final Path tmpDir) throws Exception {
        final var dsaPublicKeyPem = tmpDir.resolve("dsa_public_key.pem");
        final var dsaPrivateKeyPem = tmpDir.resolve("dsa_private_key.pem");

        final var dsaKeyPair = KeyPairGenerator.getInstance("DSA").generateKeyPair();
        writePemFile(dsaPublicKeyPem, new X509EncodedKeySpec(dsaKeyPair.getPublic().getEncoded()));
        writePemFile(dsaPrivateKeyPem, new PKCS8EncodedKeySpec(dsaKeyPair.getPrivate().getEncoded()));

        assertThatThrownBy(
            () -> RsaKeysReader.readRsaKeyPair(dsaPublicKeyPem, dsaPrivateKeyPem))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Couldn't generate RSA key pair");
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPublicKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPublicKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_public_key.pem"));

        assertThatThrownBy(
            () -> RsaKeysReader.readRsaKeyPair(emptyPublicKeyPemFile, privateKeyPem))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Couldn't read PEM file");
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPrivateKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPrivateKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_private_key.pem"));

        assertThatThrownBy(
            () -> RsaKeysReader.readRsaKeyPair(publicKeyPem, emptyPrivateKeyPemFile))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Couldn't read PEM file");
    }

}
