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

package io.aiven.kafka.tieredstorage.commons;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Random;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;

public class AesKeyAwareTest {
    public static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    public static final String CIPHER_PROVIDER = "BC";

    protected static int ivSize;
    protected static SecretKeySpec secretKey;
    protected static byte[] aad;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeAll
    static void initCrypto() {
        // These are tests, we don't need a secure source of randomness.
        final Random random = new Random();

        final byte[] encryptionKey = new byte[32];
        random.nextBytes(encryptionKey);
        secretKey = new SecretKeySpec(encryptionKey, "AES");

        aad = new byte[32];
        random.nextBytes(aad);

        ivSize = encryptionCipher().getIV().length;
    }

    protected static Cipher encryptionCipher() {
        try {
            final Cipher encryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION, CIPHER_PROVIDER);
            encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(aad);
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException
                       | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
    }

    protected static Cipher decryptionCipher(final byte[] encryptedChunk) {
        try {
            final Cipher encryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION, CIPHER_PROVIDER);
            encryptCipher.init(Cipher.DECRYPT_MODE, secretKey,
                new IvParameterSpec(encryptedChunk, 0, ivSize),
                SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(aad);
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException
                       | NoSuchPaddingException | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
    }
}
