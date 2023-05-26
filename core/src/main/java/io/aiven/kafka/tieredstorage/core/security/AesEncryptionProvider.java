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

package io.aiven.kafka.tieredstorage.core.security;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import io.aiven.kafka.tieredstorage.core.manifest.SegmentEncryptionMetadata;

public class AesEncryptionProvider implements Encryption, Decryption {

    public static final int KEY_SIZE = 512;
    public static final int KEY_AND_AAD_SIZE_BYTES = KEY_SIZE / 8 / 2;
    public static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";

    private final KeyGenerator aesKeyGenerator;

    static KeyGenerator keyGenerator() {
        try {
            final KeyGenerator kg = KeyGenerator.getInstance("AES", "BC");
            kg.init(KEY_SIZE, SecureRandom.getInstanceStrong());
            return kg;
        } catch (final NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
    }

    public AesEncryptionProvider() {
        this.aesKeyGenerator = keyGenerator();
    }

    public SecretKey createDataKey() {
        return aesKeyGenerator.generateKey();
    }

    public DataKeyAndAAD createDataKeyAndAAD() {
        final byte[] dataKeyAndAAD = createDataKey().getEncoded();
        final byte[] dataKey = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(dataKeyAndAAD, 0, dataKey, 0, 32);
        final byte[] aad = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(dataKeyAndAAD, 32, aad, 0, 32);
        return new DataKeyAndAAD(new SecretKeySpec(dataKey, "AES"), aad);
    }

    public Cipher encryptionCipher(final DataKeyAndAAD dataKeyAndAAD) {
        final Cipher encryptCipher = createEncryptingCipher(dataKeyAndAAD.dataKey, AES_TRANSFORMATION);
        encryptCipher.updateAAD(dataKeyAndAAD.aad);
        return encryptCipher;
    }

    public Cipher decryptionCipher(final byte[] encryptedChunk,
                                   final SegmentEncryptionMetadata encryptionMetadata) {
        final IvParameterSpec params = new IvParameterSpec(encryptedChunk, 0, encryptionMetadata.ivSize());
        final Cipher encryptCipher = createDecryptingCipher(encryptionMetadata.dataKey(), params, AES_TRANSFORMATION);
        encryptCipher.updateAAD(encryptionMetadata.aad());
        return encryptCipher;
    }
}
