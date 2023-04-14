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

package io.aiven.kafka.tieredstorage.commons.io;

import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.aiven.kafka.tieredstorage.commons.security.Decryption;
import io.aiven.kafka.tieredstorage.commons.security.Encryption;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

public class CryptoIOProvider implements Encryption, Decryption {

    static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";

    static final int NONCE_LENGTH = 12;

    private final SecretKey encryptionKey;

    private final int bufferSize;

    private final byte[] aad;

    public CryptoIOProvider(final SecretKey encryptionKey, final byte[] aad, final int bufferSize) {
        this.encryptionKey = encryptionKey;
        this.aad = aad;
        this.bufferSize = bufferSize;
    }

    public long compressAndEncrypt(final InputStream in,
                                   final OutputStream out) throws IOException {
        final var cipher = createEncryptingCipher(encryptionKey, CIPHER_TRANSFORMATION);
        cipher.updateAAD(aad);
        out.write(cipher.getIV());
        try (final ZstdOutputStream encrypted = new ZstdOutputStream(new CipherOutputStream(out, cipher))) {
            return IOUtils.copy(in, encrypted, bufferSize);
        }
    }

    public InputStream decryptAndDecompress(final InputStream in) throws IOException {
        final var cipher = createDecryptingCipher(
            encryptionKey,
            new IvParameterSpec(in.readNBytes(NONCE_LENGTH)),
            CIPHER_TRANSFORMATION);
        cipher.updateAAD(aad);
        return new ZstdInputStream(new CipherInputStream(in, cipher));
    }
}
