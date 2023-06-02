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

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Objects;

public final class RsaEncryptionProvider {
    private static final String RSA_TRANSFORMATION = "RSA/NONE/OAEPWithSHA3-512AndMGF1Padding";

    private final KeyPair rsaKeyPair;

    private RsaEncryptionProvider(final KeyPair rsaKeyPair) {
        this.rsaKeyPair = rsaKeyPair;
    }

    public static RsaEncryptionProvider of(final String publicKey,
                                           final RsaKeyFormat publicKeyFormat,
                                           final String privateKey,
                                           final RsaKeyFormat privateKeyFormat) {
        Objects.requireNonNull(publicKey, "publicKey cannot be null");
        Objects.requireNonNull(publicKeyFormat, "publicKeyFormat cannot be null");
        Objects.requireNonNull(privateKey, "privateKey cannot be null");
        Objects.requireNonNull(privateKeyFormat, "privateKeyFormat cannot be null");
        final KeyPair rsaKeyPair = RsaKeyReader.readKeyPair(publicKey, publicKeyFormat, privateKey, privateKeyFormat);
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
}
