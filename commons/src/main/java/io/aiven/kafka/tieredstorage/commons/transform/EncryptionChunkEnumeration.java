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
import java.util.function.Supplier;

/**
 * The chunk transformation that does encryption.
 */
public class EncryptionChunkEnumeration implements TransformChunkEnumeration {
    private final TransformChunkEnumeration inner;
    private final Supplier<Cipher> cipherSupplier;

    private final Integer transformedChunkSize;

    public EncryptionChunkEnumeration(final TransformChunkEnumeration inner,
                                      final Supplier<Cipher> cipherSupplier) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
        this.cipherSupplier = Objects.requireNonNull(cipherSupplier, "cipherSupplier cannot be null");

        final Integer innerTransformedChunkSize = inner.transformedChunkSize();
        if (innerTransformedChunkSize == null) {
            transformedChunkSize = null;
        } else {
            final Cipher cipher = cipherSupplier.get();
            transformedChunkSize = encryptedChunkSize(cipher, cipher.getIV().length, innerTransformedChunkSize);
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
        final var cipher = cipherSupplier.get();
        final var chunk = inner.nextElement();
        final byte[] iv = cipher.getIV();
        final int transformedChunkSize = encryptedChunkSize(cipher, iv.length, chunk.length);
        final byte[] transformedChunk = new byte[transformedChunkSize];
        // Prepend the IV and then write the encrypted data.
        System.arraycopy(iv, 0, transformedChunk, 0, iv.length);
        try {
            cipher.doFinal(chunk, 0, chunk.length, transformedChunk, iv.length);
        } catch (final ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
        return transformedChunk;
    }

    private int encryptedChunkSize(final Cipher cipher, final int ivSize, final int inputChunkSize) {
        return ivSize + cipher.getOutputSize(inputChunkSize);
    }
}
