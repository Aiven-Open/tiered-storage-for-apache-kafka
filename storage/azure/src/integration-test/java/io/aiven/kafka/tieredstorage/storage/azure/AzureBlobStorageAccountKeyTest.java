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

package io.aiven.kafka.tieredstorage.storage.azure;

import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.StorageBackend;

class AzureBlobStorageAccountKeyTest extends AzureBlobStorageTest {
    @Override
    protected StorageBackend storage() {
        final AzureBlobStorage azureBlobStorage = new AzureBlobStorage();
        // The well-known Azurite account name and key.
        final String accountName = "devstoreaccount1";
        final String accountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.account.name", accountName,
            "azure.account.key", accountKey,
            "azure.endpoint.url", endpoint()
        );
        azureBlobStorage.configure(configs);
        return azureBlobStorage;
    }
}
