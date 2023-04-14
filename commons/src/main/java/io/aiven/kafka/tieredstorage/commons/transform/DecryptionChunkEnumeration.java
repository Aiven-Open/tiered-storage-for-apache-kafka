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

import java.util.Objects;
import java.util.function.Function;

/**
 * The chunk de-transformation that does decryption.
 */
public class DecryptionChunkEnumeration implements DetransformChunkEnumeration {
    private final DetransformChunkEnumeration inner;
    private final int ivSize;
    private final Function<byte[], Cipher> cipherSupplier;

    /**
     * @param cipherSupplier a function that takes an encrypted chunk and returns the decryption cypher for it
     */
    public DecryptionChunkEnumeration(final DetransformChunkEnumeration inner,
                                      final int ivSize,
                                      final Function<byte[], Cipher> cipherSupplier) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
        if (ivSize <= 0) {
            throw new IllegalArgumentException("ivSize must be positive");
        }
        this.ivSize = ivSize;
        this.cipherSupplier = Objects.requireNonNull(cipherSupplier, "cipherSupplier cannot be null");
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public byte[] nextElement() {
        final var chunk = inner.nextElement();
        final var cipher = cipherSupplier.apply(chunk);
        try {
            return cipher.doFinal(chunk, ivSize, chunk.length - ivSize);
        } catch (final IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
}
