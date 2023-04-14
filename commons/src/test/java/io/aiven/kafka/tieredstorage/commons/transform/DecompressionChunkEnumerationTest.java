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

import com.github.luben.zstd.ZstdCompressCtx;
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
class DecompressionChunkEnumerationTest {
    @Mock
    DetransformChunkEnumeration inner;

    @Test
    void nullInnerEnumeration() {
        assertThatThrownBy(() -> new DecompressionChunkEnumeration(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inner cannot be null");
    }

    @Test
    void hasMoreElementsPropagated() {
        final var transform = new DecompressionChunkEnumeration(inner);
        when(inner.hasMoreElements())
            .thenReturn(true)
            .thenReturn(false);
        assertThat(transform.hasMoreElements()).isTrue();
        assertThat(transform.hasMoreElements()).isFalse();
        verify(inner, times(2)).hasMoreElements();
    }

    @Test
    void decompress() {
        final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final byte[] compressed;
        try (final ZstdCompressCtx compressCtx = new ZstdCompressCtx()) {
            compressCtx.setContentSize(true);
            compressed = compressCtx.compress(data);
        }

        final var transform = new DecompressionChunkEnumeration(inner);
        when(inner.nextElement()).thenReturn(compressed);

        assertThat(transform.nextElement()).isEqualTo(data);
    }
}
