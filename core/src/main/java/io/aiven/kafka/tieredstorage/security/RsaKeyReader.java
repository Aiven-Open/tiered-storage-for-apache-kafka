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

package io.aiven.kafka.tieredstorage.security;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Objects;

import org.bouncycastle.util.io.pem.PemReader;

public class RsaKeyReader {
    static KeyPair readKeyPair(final String publicKey,
                               final RsaKeyFormat publicKeyFormat,
                               final String privateKey,
                               final RsaKeyFormat privateKeyFormat) {
        final KeyFactory keyFactory;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        try {
            final byte[] publicKeyPemContent = readPemContent(publicKey, publicKeyFormat);
            final var publicKeySpec = new X509EncodedKeySpec(publicKeyPemContent);

            final byte[] privateKeyPemContent = readPemContent(privateKey, privateKeyFormat);
            final var privateKeySpec = new PKCS8EncodedKeySpec(privateKeyPemContent);

            return new KeyPair(
                keyFactory.generatePublic(publicKeySpec),
                keyFactory.generatePrivate(privateKeySpec));
        } catch (final IOException | InvalidKeySpecException e) {
            throw new RuntimeException("Couldn't read RSA key pair", e);
        }
    }

    private static byte[] readPemContent(final String key,
                                         final RsaKeyFormat keyFormat) throws IOException {
        final InputStream inputStream;
        if (keyFormat == RsaKeyFormat.PEM) {
            inputStream = new ByteArrayInputStream(key.getBytes());
        } else if (keyFormat == RsaKeyFormat.PEM_FILE) {
            inputStream = Files.newInputStream(Path.of(key));
        } else {
            throw new IllegalArgumentException("Unsupported RSA key format: " + keyFormat);
        }

        try (inputStream;
             final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             final var pemReader = new PemReader(inputStreamReader)) {
            final var pemObject = pemReader.readPemObject();
            if (Objects.isNull(pemObject)) {
                throw new IllegalArgumentException("Couldn't read PEM");
            }
            return pemObject.getContent();
        } catch (final IOException e) {
            throw new IllegalArgumentException("Couldn't read PEM", e);
        }
    }
}
