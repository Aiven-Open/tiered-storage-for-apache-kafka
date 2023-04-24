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

package io.aiven.kafka.tieredstorage.commons.transform;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import java.util.Objects;

import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionProvider;

/**
 * The chunk transformation that does encryption.
 */
public class EncryptionChunkEnumeration implements TransformChunkEnumeration {
    private final TransformChunkEnumeration inner;

    private Integer transformedChunkSize;
    private final EncryptionProvider encryptionProvider;
    private final DataKeyAndAAD dataKeyAndAAD;

    public EncryptionChunkEnumeration(final TransformChunkEnumeration inner,
                                      final EncryptionProvider encryptionProvider,
                                      final DataKeyAndAAD dataKeyAndAAD) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
        this.encryptionProvider = Objects.requireNonNull(encryptionProvider, "encryptionProvider cannot be null");
        this.dataKeyAndAAD = Objects.requireNonNull(dataKeyAndAAD, "dataKeyAndAAD cannot be null");

        final Integer innerTransformedChunkSize = inner.transformedChunkSize();
        if (innerTransformedChunkSize == null) {
            transformedChunkSize = null;
        } else {
            transformedChunkSize = encryptedChunkSize(
                encryptionProvider.encryptionCipher(dataKeyAndAAD),
                innerTransformedChunkSize
            );
        }
    }

    @Override
    public int originalChunkSize() {
        return inner.originalChunkSize();
    }

    @Override
    public Integer transformedChunkSize() {
        return this.transformedChunkSize;
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public byte[] nextElement() {
        final var chunk = inner.nextElement();
        final Cipher cipher = encryptionProvider.encryptionCipher(dataKeyAndAAD);
        transformedChunkSize = encryptedChunkSize(cipher, chunk.length);
        final byte[] transformedChunk = new byte[transformedChunkSize];
        final byte[] iv = cipher.getIV();
        // Prepend the IV and then write the encrypted data.
        System.arraycopy(cipher.getIV(), 0, transformedChunk, 0, iv.length);
        try {
            cipher.doFinal(chunk, 0, chunk.length, transformedChunk, iv.length);
        } catch (final ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
        return transformedChunk;
    }

    private static int encryptedChunkSize(final Cipher cipher, final int inputChunkSize) {
        return cipher.getIV().length + cipher.getOutputSize(inputChunkSize);
    }
}
