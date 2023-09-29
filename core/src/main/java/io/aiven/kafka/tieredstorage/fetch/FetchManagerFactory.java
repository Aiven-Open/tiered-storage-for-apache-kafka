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

package io.aiven.kafka.tieredstorage.fetch;

import java.util.Map;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.fetch.cache.FetchCache;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;

public class FetchManagerFactory implements Configurable {
    private FetchManagerFactoryConfig config;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new FetchManagerFactoryConfig(configs);
    }

    public FetchManager initChunkManager(final ObjectFetcher fileFetcher,
                                         final AesEncryptionProvider aesEncryptionProvider) {
        final DefaultFetchManager defaultFetchManager = new DefaultFetchManager(fileFetcher, aesEncryptionProvider);
        if (config.cacheClass() != null) {
            try {
                final FetchCache<?> fetchCache = config
                    .cacheClass()
                    .getDeclaredConstructor(FetchManager.class)
                    .newInstance(defaultFetchManager);
                fetchCache.configure(config.originalsWithPrefix(FetchManagerFactoryConfig.FETCH_CACHE_PREFIX));
                return fetchCache;
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return defaultFetchManager;
        }
    }
}
