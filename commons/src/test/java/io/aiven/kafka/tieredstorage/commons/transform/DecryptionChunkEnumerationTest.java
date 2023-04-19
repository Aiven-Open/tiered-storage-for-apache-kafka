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

import io.aiven.kafka.tieredstorage.commons.AesKeyAwareTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DecryptionChunkEnumerationTest extends AesKeyAwareTest {
    @Mock
    DetransformChunkEnumeration inner;

    @Mock
    Cipher cipher;

    Cipher cipherSupplier(final byte[] encryptedChunk) {
        return cipher;
    }

    @Test
    void nullInnerEnumeration() {
        assertThatThrownBy(() -> new DecryptionChunkEnumeration(null, 10, this::cipherSupplier))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inner cannot be null");
    }

    @Test
    void zeroIvSize() {
        assertThatThrownBy(() -> new DecryptionChunkEnumeration(inner, 0, this::cipherSupplier))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("ivSize must be positive");
    }

    @Test
    void nullCipherSupplier() {
        assertThatThrownBy(() -> new DecryptionChunkEnumeration(inner, ivSize, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("cipherSupplier cannot be null");
    }

    @Test
    void hasMoreElementsPropagated() {
        final var transform = new DecryptionChunkEnumeration(inner, ivSize, this::cipherSupplier);
        when(inner.hasMoreElements())
            .thenReturn(true)
            .thenReturn(false);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.hasMoreElements()).isFalse();
        verify(inner, times(2)).hasMoreElements();
    }

    @Test
    void decrypt() throws IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final Cipher encryptionCipher = AesKeyAwareTest.encryptionCipher();
        final byte[] iv = encryptionCipher.getIV();
        final byte[] encrypted = new byte[iv.length + encryptionCipher.getOutputSize(data.length)];
        System.arraycopy(iv, 0, encrypted, 0, iv.length);
        encryptionCipher.doFinal(data, 0, data.length, encrypted, iv.length);

        final var transform = new DecryptionChunkEnumeration(inner, ivSize, AesKeyAwareTest::decryptionCipher);
        when(inner.nextElement()).thenReturn(encrypted);
        final byte[] decrypted = transform.nextElement();

        assertThat(decrypted).isEqualTo(data);
    }
}
