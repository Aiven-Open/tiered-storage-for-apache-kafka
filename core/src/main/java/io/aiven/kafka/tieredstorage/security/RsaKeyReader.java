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
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
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
        try {
            final PublicKey publicKeyRead;
            try (final var is = getInputStream(publicKey, publicKeyFormat)) {
                publicKeyRead = readPublicKey(is);
            }
            final PrivateKey privateKeyRead;
            try (final var is = getInputStream(privateKey, privateKeyFormat)) {
                privateKeyRead = readPrivateKey(is);
            }
            return new KeyPair(publicKeyRead, privateKeyRead);
        } catch (final IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Couldn't read RSA key pair", e);
        }
    }

    private static InputStream getInputStream(final String key,
                                              final RsaKeyFormat keyFormat) throws IOException {
        if (keyFormat == RsaKeyFormat.PEM) {
            return new ByteArrayInputStream(key.getBytes());
        } else if (keyFormat == RsaKeyFormat.PEM_FILE) {
            return Files.newInputStream(Path.of(key));
        } else {
            throw new IllegalArgumentException("Unsupported RSA key format: " + keyFormat);
        }
    }

    private static PublicKey readPublicKey(final InputStream in)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
        final var pemContent = readPemContent(new InputStreamReader(in));
        final var keySpec = new X509EncodedKeySpec(pemContent);
        final var kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(keySpec);
    }

    private static PrivateKey readPrivateKey(final InputStream in)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
        final var pemContent = readPemContent(new InputStreamReader(in));
        final var keySpec = new PKCS8EncodedKeySpec(pemContent);
        final var kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    private static byte[] readPemContent(final Reader reader) {
        try (final var pemReader = new PemReader(reader)) {
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
