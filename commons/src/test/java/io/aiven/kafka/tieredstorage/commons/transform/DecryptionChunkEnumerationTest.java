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

import io.aiven.kafka.tieredstorage.commons.EncryptionAwareTest;
import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;

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
class DecryptionChunkEnumerationTest extends EncryptionAwareTest {
    @Mock
    DetransformChunkEnumeration inner;

    @Test
    void nullInnerEnumeration() {
        assertThatThrownBy(() ->
            new DecryptionChunkEnumeration(
                null,
                encryptionProvider,
                encryptionProvider.createDataKeyAndAAD()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inner cannot be null");
    }

    @Test
    void nullEncryptionProvider() {
        assertThatThrownBy(() ->
            new DecryptionChunkEnumeration(
                inner,
                null,
                encryptionProvider.createDataKeyAndAAD()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("encryptionProvider cannot be null");
    }

    @Test
    void nullDataKeyAndAAD() {
        assertThatThrownBy(() ->
            new DecryptionChunkEnumeration(
                inner,
                encryptionProvider,
                null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("dataKeyAndAAD cannot be null");
    }

    @Test
    void hasMoreElementsPropagated() {
        final var transform = new DecryptionChunkEnumeration(
            inner,
            encryptionProvider,
            encryptionProvider.createDataKeyAndAAD());
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
        final DataKeyAndAAD dataKeyAndAAD = encryptionProvider.createDataKeyAndAAD();
        final Cipher encryptionCipher = encryptionProvider.encryptionCipher(dataKeyAndAAD);
        final byte[] iv = encryptionCipher.getIV();
        final byte[] encrypted = new byte[iv.length + encryptionCipher.getOutputSize(data.length)];
        System.arraycopy(iv, 0, encrypted, 0, iv.length);
        encryptionCipher.doFinal(data, 0, data.length, encrypted, iv.length);

        final var transform = new DecryptionChunkEnumeration(
            inner,
            encryptionProvider,
            dataKeyAndAAD
        );
        when(inner.nextElement()).thenReturn(encrypted);
        final byte[] decrypted = transform.nextElement();

        assertThat(decrypted).isEqualTo(data);
    }
}
