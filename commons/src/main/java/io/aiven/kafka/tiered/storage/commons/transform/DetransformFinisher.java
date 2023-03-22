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

package io.aiven.kafka.tiered.storage.commons.transform;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;

/**
 * The detransformation finisher.
 *
 * <p>It converts enumeration of {@code byte[]} into enumeration of {@link InputStream},
 * so that it could be used in {@link SequenceInputStream}.
 */
public class DetransformFinisher implements Enumeration<InputStream> {
    private final DetransformChunkEnumeration inner;

    public DetransformFinisher(final DetransformChunkEnumeration inner) {
        this.inner = inner;
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public InputStream nextElement() {
        final var chunk = inner.nextElement();
        return new ByteArrayInputStream(chunk);
    }
}
