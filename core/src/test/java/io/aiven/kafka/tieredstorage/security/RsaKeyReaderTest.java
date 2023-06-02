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

package io.aiven.kafka.tieredstorage.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import io.aiven.kafka.tieredstorage.RsaKeyAwareTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RsaKeyReaderTest extends RsaKeyAwareTest {

    @Test
    public void bothFromString() {
        final KeyPair readKeyPair = RsaKeyReader.readKeyPair(
            publicKeyPem, RsaKeyFormat.PEM, privateKeyPem, RsaKeyFormat.PEM);
        assertThat(readKeyPair.getPublic()).isEqualTo(rsaKeyPair.getPublic());
        assertThat(readKeyPair.getPrivate()).isEqualTo(rsaKeyPair.getPrivate());
    }

    @Test
    public void publicFromStringPrivateFromFile(@TempDir final Path tmpDir) throws IOException {
        final Path privateKeyFile = tmpDir.resolve("private.pem");
        Files.writeString(privateKeyFile, privateKeyPem);
        final KeyPair readKeyPair = RsaKeyReader.readKeyPair(
            publicKeyPem, RsaKeyFormat.PEM, privateKeyFile.toString(), RsaKeyFormat.PEM_FILE);
        assertThat(readKeyPair.getPublic()).isEqualTo(rsaKeyPair.getPublic());
        assertThat(readKeyPair.getPrivate()).isEqualTo(rsaKeyPair.getPrivate());
    }

    @Test
    public void publicFromFilePrivateFromString(@TempDir final Path tmpDir) throws IOException {
        final Path publicKeyFile = tmpDir.resolve("public.pem");
        Files.writeString(publicKeyFile, publicKeyPem);
        final KeyPair readKeyPair = RsaKeyReader.readKeyPair(
            publicKeyFile.toString(), RsaKeyFormat.PEM_FILE, privateKeyPem, RsaKeyFormat.PEM);
        assertThat(readKeyPair.getPublic()).isEqualTo(rsaKeyPair.getPublic());
        assertThat(readKeyPair.getPrivate()).isEqualTo(rsaKeyPair.getPrivate());
    }

    @Test
    public void bothFromStringFile(@TempDir final Path tmpDir) throws IOException {
        final Path publicKeyFile = tmpDir.resolve("public.pem");
        Files.writeString(publicKeyFile, publicKeyPem);
        final Path privateKeyFile = tmpDir.resolve("private.pem");
        Files.writeString(privateKeyFile, privateKeyPem);
        final KeyPair readKeyPair = RsaKeyReader.readKeyPair(
            publicKeyFile.toString(), RsaKeyFormat.PEM_FILE, privateKeyFile.toString(), RsaKeyFormat.PEM_FILE);
        assertThat(readKeyPair.getPublic()).isEqualTo(rsaKeyPair.getPublic());
        assertThat(readKeyPair.getPrivate()).isEqualTo(rsaKeyPair.getPrivate());
    }

    @Test
    public void failsForUnknownPaths() {
        assertThatThrownBy(
            () -> RsaKeyReader.readKeyPair("./some", RsaKeyFormat.PEM_FILE, "./path", RsaKeyFormat.PEM_FILE))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Couldn't read RSA key pair");
    }

    @Test
    void throwsIllegalArgumentExceptionUnsupportedKey() throws Exception {
        final var dsaKeyPair = KeyPairGenerator.getInstance("DSA").generateKeyPair();
        final String dsaPublicKeyPem = encodeAsPem(new X509EncodedKeySpec(dsaKeyPair.getPublic().getEncoded()));
        final var dsaPrivateKeyPem = encodeAsPem(new PKCS8EncodedKeySpec(dsaKeyPair.getPrivate().getEncoded()));
        assertThatThrownBy(
            () -> RsaKeyReader.readKeyPair(dsaPublicKeyPem, RsaKeyFormat.PEM, dsaPrivateKeyPem, RsaKeyFormat.PEM))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Couldn't read RSA key pair");
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPublicKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPublicKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_public_key.pem"));

        assertThatThrownBy(
            () -> RsaKeyReader.readKeyPair(
                emptyPublicKeyPemFile.toString(), RsaKeyFormat.PEM_FILE, privateKeyPem, RsaKeyFormat.PEM))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Couldn't read PEM");
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyPrivateKey(@TempDir final Path tmpDir) throws IOException {
        final var emptyPrivateKeyPemFile =
            Files.createFile(tmpDir.resolve("empty_private_key.pem"));

        assertThatThrownBy(
            () -> RsaKeyReader.readKeyPair(
                publicKeyPem, RsaKeyFormat.PEM, emptyPrivateKeyPemFile.toString(), RsaKeyFormat.PEM_FILE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Couldn't read PEM");
    }

}
