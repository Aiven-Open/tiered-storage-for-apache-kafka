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

package io.aiven.kafka.tieredstorage.transform;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DetransformFinisherTest {

    @Test
    void sameInputStreamFromBaseDetransform() {
        final byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        final var is = new ByteArrayInputStream(bytes);
        final DetransformChunkEnumeration d = new BaseDetransformChunkEnumeration(is);
        final DetransformFinisher f = new DetransformFinisher(d);
        assertThat(f.toInputStream())
            .isEqualTo(is)
            .hasBinaryContent(bytes);
    }

    @Test
    void inputStreamMutatesWhenChainedDetransform() {
        final byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        final var is = new ByteArrayInputStream(bytes);
        final DetransformChunkEnumeration d = new BaseDetransformChunkEnumeration(is);
        final DetransformChunkEnumeration noop = new NoopDetransformEnumeration(d);
        final DetransformFinisher f = new DetransformFinisher(noop);
        assertThat(f.toInputStream())
            .isNotEqualTo(is)
            .hasBinaryContent(bytes);
    }

    private static class NoopDetransformEnumeration implements DetransformChunkEnumeration {
        private final DetransformChunkEnumeration inner;

        public NoopDetransformEnumeration(final DetransformChunkEnumeration inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasMoreElements() {
            return inner.hasMoreElements();
        }

        @Override
        public byte[] nextElement() {
            return inner.nextElement();
        }
    }
}
