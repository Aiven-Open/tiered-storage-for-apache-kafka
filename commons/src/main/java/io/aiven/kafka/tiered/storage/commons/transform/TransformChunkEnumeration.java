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

import java.util.Enumeration;

/**
 * The enumeration of chunks being transformed.
 *
 * <p>There are supposed to be multiple implementation doing different transformations
 * (like compression and encryption).
 * These implementations are supposed to be composable.
 */
public interface TransformChunkEnumeration extends Enumeration<byte[]> {
    /**
     * Returns the original (i.e. before all the transformations) chunk size.
     *
     * <p>Normally it should be propagated through the chain of transformations.
     */
    int originalChunkSize();

    /**
     * Returns a transformed chunk size if it's known.
     * @return a transformed chunk size; or {@code null} if unknown.
     */
    Integer transformedChunkSize();
}
