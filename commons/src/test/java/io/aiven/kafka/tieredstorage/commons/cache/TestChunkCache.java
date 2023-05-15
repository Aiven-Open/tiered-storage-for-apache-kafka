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

package io.aiven.kafka.tieredstorage.commons.cache;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.commons.ChunkKey;

public class TestChunkCache implements ChunkCache {
    public boolean configureCalled;
    public Map<String, ?> configuredWith;

    @Override
    public Optional<InputStream> get(final ChunkKey chunkKey) {
        return Optional.empty();
    }

    @Override
    public String storeTemporarily(final byte[] chunk) {
        return null;
    }

    @Override
    public void store(final String tempId, final ChunkKey chunkKey) {

    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.configureCalled = true;
        this.configuredWith = configs;
    }
}
