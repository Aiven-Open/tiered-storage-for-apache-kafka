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

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.manifest.SegmentEncryptionMetadata;

public class AesEncryptionProvider {

    public static final int KEY_SIZE = 256;
    // By default, equals the AES block size, see GaloisCounterMode.DEFAULT_TAG_LEN.
    public static final int GCM_TAG_LENGTH = 128;
    public static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";

    private final KeyGenerator aesKeyGenerator;

    public AesEncryptionProvider() {
        try {
            this.aesKeyGenerator = KeyGenerator.getInstance("AES");
            this.aesKeyGenerator.init(KEY_SIZE, SecureRandom.getInstanceStrong());
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public DataKeyAndAAD createDataKeyAndAAD() {
        // Attention: these two generateKey() calls must be separate!
        // "Optimizing" them into one will result in a serious security flaw.
        final var dataKey = aesKeyGenerator.generateKey();
        final byte[] aad = aesKeyGenerator.generateKey().getEncoded();
        return new DataKeyAndAAD(dataKey, aad);
    }

    public Cipher encryptionCipher(final DataKeyAndAAD dataKeyAndAAD) {
        final Cipher encryptCipher = createEncryptingCipher(dataKeyAndAAD.dataKey);
        encryptCipher.updateAAD(dataKeyAndAAD.aad);
        return encryptCipher;
    }

    private Cipher createEncryptingCipher(final Key key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            final var cipher = Cipher.getInstance(AES_TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, SecureRandom.getInstanceStrong());
            return cipher;
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new RuntimeException("Couldn't create encrypt cipher", e);
        }
    }

    public Cipher decryptionCipher(final byte[] encryptedChunk,
                                   final SegmentEncryptionMetadata encryptionMetadata) {
        final GCMParameterSpec params = new GCMParameterSpec(
            GCM_TAG_LENGTH, encryptedChunk, 0, encryptionMetadata.ivSize());
        final Cipher encryptCipher = createDecryptingCipher(encryptionMetadata.dataKey(), params);
        encryptCipher.updateAAD(encryptionMetadata.aad());
        return encryptCipher;
    }

    private Cipher createDecryptingCipher(final Key key,
                                          final AlgorithmParameterSpec params) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(params, "params cannot be null");
        try {
            final var cipher = Cipher.getInstance(AES_TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, params, SecureRandom.getInstanceStrong());
            return cipher;
        } catch (final NoSuchAlgorithmException | NoSuchPaddingException
                       | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException("Couldn't create decrypt cipher", e);
        }
    }
}
