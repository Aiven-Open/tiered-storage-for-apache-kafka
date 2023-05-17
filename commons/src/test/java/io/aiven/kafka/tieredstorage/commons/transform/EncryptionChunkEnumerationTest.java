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
class EncryptionChunkEnumerationTest extends EncryptionAwareTest {
    @Mock
    TransformChunkEnumeration inner;

    @Test
    void nullInnerEnumeration() {
        assertThatThrownBy(() ->
            new EncryptionChunkEnumeration(
                null,
                encryptionProvider,
                encryptionProvider.createDataKeyAndAAD()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inner cannot be null");
    }

    @Test
    void nullEncryptionProvider() {
        assertThatThrownBy(() ->
            new EncryptionChunkEnumeration(
                inner,
                null,
                encryptionProvider.createDataKeyAndAAD()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("encryptionProvider cannot be null");
    }

    @Test
    void nullDataKeyAndAAD() {
        assertThatThrownBy(() ->
            new EncryptionChunkEnumeration(
                inner,
                encryptionProvider,
                null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("dataKeyAndAAD cannot be null");
    }

    @Test
    void originalChunkSizePropagated() {
        when(inner.originalChunkSize()).thenReturn(100);
        final var transform = new EncryptionChunkEnumeration(
            inner,
            encryptionProvider,
            encryptionProvider.createDataKeyAndAAD()
        );
        assertThat(transform.originalChunkSize()).isEqualTo(100);
        verify(inner).originalChunkSize();
    }

    @Test
    void transformedChunkSizeIsPropagatedWhenNull() {
        when(inner.transformedChunkSize()).thenReturn(null);
        final var transform = new EncryptionChunkEnumeration(
            inner,
            encryptionProvider,
            encryptionProvider.createDataKeyAndAAD()
        );
        assertThat(transform.transformedChunkSize()).isNull();
        verify(inner).transformedChunkSize();
    }

    @Test
    void transformedChunkSizeIsCalculatedWhenNotNull() {
        when(inner.transformedChunkSize()).thenReturn(100);
        final var transform = new EncryptionChunkEnumeration(
            inner,
            encryptionProvider,
            encryptionProvider.createDataKeyAndAAD()
        );
        //TODO explain why 128 out of 100
        assertThat(transform.transformedChunkSize()).isEqualTo(128);
    }

    @Test
    void hasMoreElementsPropagated() {
        when(inner.transformedChunkSize()).thenReturn(null);
        final var transform = new EncryptionChunkEnumeration(
            inner,
            encryptionProvider,
            encryptionProvider.createDataKeyAndAAD()
        );
        when(inner.hasMoreElements())
            .thenReturn(true)
            .thenReturn(false);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.hasMoreElements()).isFalse();
        verify(inner, times(2)).hasMoreElements();
    }

    @Test
    void encrypt() throws IllegalBlockSizeException, BadPaddingException {
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        final DataKeyAndAAD dataKeyAndAAD = encryptionProvider.createDataKeyAndAAD();
        final var transform = new EncryptionChunkEnumeration(
            inner,
            encryptionProvider,
            dataKeyAndAAD
        );
        when(inner.nextElement()).thenReturn(data);
        final byte[] encrypted = transform.nextElement();

        final Cipher decryptCipher = encryptionProvider.decryptionCipher(encrypted, dataKeyAndAAD);
        assertThat(decryptCipher.doFinal(encrypted, ivSize, encrypted.length - ivSize)).isEqualTo(data);
    }
}
