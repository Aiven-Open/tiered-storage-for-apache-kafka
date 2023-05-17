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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.nio.file.Path;

public class EncryptionProvider {

    final RsaEncryption rsaEncryptionProvider;
    final AesEncryption aesEncryption;

    public EncryptionProvider(final RsaEncryption rsaEncryption) {
        this.rsaEncryptionProvider = rsaEncryption;
        this.aesEncryption = new AesEncryption();
    }

    public static EncryptionProvider of(final Path publicKey, final Path privateKey) {
        return new EncryptionProvider(RsaEncryption.of(publicKey, privateKey));
    }

    public byte[] encryptDataKey(final SecretKey dataKey) {
        return rsaEncryptionProvider.encryptDataKey(dataKey);
    }

    public SecretKey decryptDataKey(final byte[] bytes) {

        return new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(bytes), "AES");
    }

    public DataKeyAndAAD createDataKeyAndAAD() {
        return aesEncryption.createDataKeyAndAAD();
    }

    public Cipher encryptionCipher(final DataKeyAndAAD dataKeyAndAAD) {
        return aesEncryption.encryptionCipher(dataKeyAndAAD);
    }

    public Cipher decryptionCipher(final byte[] encryptedChunk, final DataKeyAndAAD dataKeyAndAAD) {
        return aesEncryption.decryptionCipher(encryptedChunk, dataKeyAndAAD);
    }

    public int ivSize() {
        return AesEncryption.IV_SIZE;
    }
}
