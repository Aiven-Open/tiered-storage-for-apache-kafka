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

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Map;
import java.util.Objects;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public final class RsaEncryptionProvider {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final String RSA_TRANSFORMATION = "RSA/NONE/OAEPWithSHA3-512AndMGF1Padding";

    private final String activeKeyId;
    private final Map<String, KeyPair> keyring;
    private final PublicKey encryptionPublicKey;

    public RsaEncryptionProvider(final String activeKeyId, final Map<String, KeyPair> keyring) {
        this.activeKeyId = Objects.requireNonNull(activeKeyId, "keyId cannot be null");
        this.keyring = Objects.requireNonNull(keyring, "keyring cannot be null");
        this.encryptionPublicKey = keyring.get(activeKeyId).getPublic();
        if (encryptionPublicKey == null) {
            throw new IllegalArgumentException("Key ID " + activeKeyId + " is not in keyring");
        }
    }

    public EncryptedDataKey encryptDataKey(final byte[] dataKey) {
        try {
            final var cipher = createEncryptingCipher(encryptionPublicKey);
            return new EncryptedDataKey(activeKeyId, cipher.doFinal(dataKey));
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

    public byte[] decryptDataKey(final EncryptedDataKey encryptedKeyString) {
        try {
            final KeyPair keyPair = keyring.get(encryptedKeyString.keyEncryptionKeyId);
            if (keyPair == null) {
                throw new IllegalArgumentException("Unknown key " + encryptedKeyString.keyEncryptionKeyId);
            }

            final Cipher cipher = createDecryptingCipher(keyPair.getPrivate());
            return cipher.doFinal(encryptedKeyString.encryptedDataKey);
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
