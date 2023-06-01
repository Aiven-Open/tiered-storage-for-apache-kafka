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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Objects;

import org.bouncycastle.util.io.pem.PemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RsaEncryptionProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(RsaEncryptionProvider.class);


    private static final String RSA_TRANSFORMATION = "RSA/NONE/OAEPWithSHA3-512AndMGF1Padding";


    private final KeyPair rsaKeyPair;

    private RsaEncryptionProvider(final KeyPair rsaKeyPair) {
        this.rsaKeyPair = rsaKeyPair;
    }

    public static RsaEncryptionProvider of(final Path rsaPublicKey, final Path rsaPrivateKey) {
        LOGGER.info("Read RSA keys");
        Objects.requireNonNull(rsaPublicKey, "rsaPublicKey hasn't been set");
        Objects.requireNonNull(rsaPrivateKey, "rsaPrivateKey hasn't been set");
        final KeyPair rsaKeyPair = RsaKeysReader.readRsaKeyPair(rsaPublicKey, rsaPrivateKey);
        return new RsaEncryptionProvider(rsaKeyPair);
    }

    public byte[] encryptDataKey(final SecretKey dataKey) {
        try {
            final var cipher = createEncryptingCipher(rsaKeyPair.getPublic());
            return cipher.doFinal(dataKey.getEncoded());
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't encrypt AES key", e);
        }
    }

    private Cipher createEncryptingCipher(final Key key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            final var cipher = Cipher.getInstance(RSA_TRANSFORMATION, "BC");
            cipher.init(Cipher.ENCRYPT_MODE, key, SecureRandom.getInstanceStrong());
            return cipher;
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException
                       | InvalidKeyException | NoSuchProviderException e) {
            throw new RuntimeException("Couldn't create encrypt cipher", e);
        }
    }

    public byte[] decryptDataKey(final byte[] bytes) {
        try {
            final Cipher cipher = createDecryptingCipher(rsaKeyPair.getPrivate());
            return cipher.doFinal(bytes);
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't decrypt AES key", e);
        }
    }

    private Cipher createDecryptingCipher(final Key key) {
        try {
            final var cipher = Cipher.getInstance(RSA_TRANSFORMATION, "BC");
            cipher.init(Cipher.DECRYPT_MODE, key, SecureRandom.getInstanceStrong());
            return cipher;
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException
                       | InvalidKeyException | NoSuchProviderException e) {
            throw new RuntimeException("Couldn't create decrypt cipher", e);
        }
    }

    static class RsaKeysReader {

        static KeyPair readRsaKeyPair(final Path publicKeyPath, final Path privateKeyPath) {
            try (final InputStream publicKeyIn = Files.newInputStream(publicKeyPath);
                 final InputStream privateKeyIn = Files.newInputStream(privateKeyPath)) {
                return readRsaKeyPair(publicKeyIn, privateKeyIn);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Couldn't read RSA key pair paths", e);
            }
        }

        static KeyPair readRsaKeyPair(final InputStream publicKeyIn, final InputStream privateKeyIn) {
            try {
                final var publicKey = readPublicKey(publicKeyIn);
                final var privateKey = readPrivateKey(privateKeyIn);
                return new KeyPair(publicKey, privateKey);
            } catch (final NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new IllegalArgumentException("Couldn't read RSA key pair", e);
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
                    throw new IllegalArgumentException("Couldn't read PEM file");
                }
                return pemObject.getContent();
            } catch (final IOException e) {
                throw new IllegalArgumentException("Couldn't read PEM file", e);
            }
        }
    }
}
