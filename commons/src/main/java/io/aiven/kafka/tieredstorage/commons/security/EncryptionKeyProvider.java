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

import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EncryptionKeyProvider
        implements Encryption, Decryption {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionKeyProvider.class);

    public static final int KEY_SIZE = 512;
    public static final int KEY_AND_AAD_SIZE_BYTES = KEY_SIZE / 8 / 2;

    private static final String CIPHER_TRANSFORMATION = "RSA/NONE/OAEPWithSHA3-512AndMGF1Padding";

    private final KeyGenerator aesKeyGenerator;

    private final KeyPair rsaKeyPair;

    private EncryptionKeyProvider(final KeyPair rsaKeyPair,
                                  final KeyGenerator aesKeyGenerator) {
        this.rsaKeyPair = rsaKeyPair;
        this.aesKeyGenerator = aesKeyGenerator;
    }

    public static EncryptionKeyProvider of(final InputStream rsaPublicKey,
                                           final InputStream rsaPrivateKey) {
        LOGGER.info("Read RSA keys");
        Objects.requireNonNull(rsaPublicKey, "rsaPublicKey hasn't been set");
        Objects.requireNonNull(rsaPrivateKey, "rsaPrivateKey hasn't been set");
        try {
            final var rsaKeyPair = RsaKeysReader.readRsaKeyPair(rsaPublicKey, rsaPrivateKey);
            final var kg = KeyGenerator.getInstance("AES", "BC");
            kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
            return new EncryptionKeyProvider(rsaKeyPair, kg);
        } catch (final NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new RuntimeException("Couldn't create encrypt key provider", e);
        }
    }

    public SecretKey createKey() {
        return aesKeyGenerator.generateKey();
    }

    public EncryptionKeyAndAAD createKeyAndAAD() {
        final byte[] keyAndAAD = createKey().getEncoded();
        final byte[] encryptionKey = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(keyAndAAD, 0, encryptionKey, 0, 32);
        final byte[] aad = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(keyAndAAD, 32, aad, 0, 32);
        return new EncryptionKeyAndAAD(new SecretKeySpec(encryptionKey, "AES"), aad);
    }

    public byte[] encryptKey(final SecretKey secretKey) {
        try {
            final var cipher = getEncryptingCipher();
            return cipher.doFinal(secretKey.getEncoded());
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't encrypt AES key", e);
        }
    }

    public Cipher getEncryptingCipher() {
        return createEncryptingCipher(rsaKeyPair.getPublic(), CIPHER_TRANSFORMATION);
    }

    public byte[] decryptKey(final byte[] bytes) {
        try {
            final Cipher cipher = getDecryptionCipher(bytes, getEncryptingCipher().getIV().length);
            return cipher.doFinal(bytes);
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Couldn't decrypt AES key", e);
        }
    }

    public Cipher getDecryptionCipher(byte[] bytes, int ivSize) {
        final AlgorithmParameterSpec params = new IvParameterSpec(bytes, 0, ivSize);
        return createDecryptingCipher(rsaKeyPair.getPrivate(), params, CIPHER_TRANSFORMATION);
    }

}
