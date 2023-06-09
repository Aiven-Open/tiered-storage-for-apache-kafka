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

import java.io.InputStream;
import java.util.function.Function;

public final class InboundTransformChain {
    final int originalSize;
    TransformChunkEnumeration inner;

    InboundTransformChain(final InputStream content, final int size, final int chunkSize) {
        this.originalSize = size;
        this.inner = new BaseTransformChunkEnumeration(content, chunkSize);
    }

    void chain(final Function<TransformChunkEnumeration, TransformChunkEnumeration> transformSupplier) {
        this.inner = transformSupplier.apply(this.inner);
    }

    public TransformFinisher complete() {
        return new TransformFinisher(inner, originalSize);
    }
}
