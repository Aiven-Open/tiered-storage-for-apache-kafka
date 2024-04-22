/*
 * Copyright 2024 Aiven Oy
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

import java.util.List;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseSocks5Test;
import io.aiven.kafka.tieredstorage.storage.TestUtils;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.azuriteContainer;
import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.connectionString;

@Testcontainers
class AzureBlobStorageSocks5Test extends BaseSocks5Test<AzureBlobStorage> {
    static final Network NETWORK = Network.newNetwork();

    static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT)
        .withNetwork(NETWORK);

    static String azuriteContainerNetworkAlias = AZURITE_SERVER.getNetworkAliases().get(0);

    @Container
    static final GenericContainer<?> PROXY_AUTHENTICATED = proxyContainer(true).withNetwork(NETWORK);
    @Container
    static final GenericContainer<?> PROXY_UNAUTHENTICATED = proxyContainer(false).withNetwork(NETWORK);

    static BlobServiceClient blobServiceClient;

    // This is an Azure container (AKA bucket), not a Docker container.
    protected String azureContainerName;

    // This endpoint points to Azurite with the Docker-internal hostname and port.
    // The host running these tests cannot reach it directly, only via a proxy.
    static String internalConnectionString;

    @BeforeAll
    static void setUpClass() {
        final String connectionStringWithMappedPort = connectionString(AZURITE_SERVER, BLOB_STORAGE_PORT);
        blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionStringWithMappedPort)
            .buildClient();

        internalConnectionString = "DefaultEndpointsProtocol=http;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=" + String.format("http://%s:%d/devstoreaccount1", azuriteContainerNetworkAlias, BLOB_STORAGE_PORT) + ";";
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        azureContainerName = TestUtils.testNameToBucketName(testInfo);
        blobServiceClient.createBlobContainer(azureContainerName);
    }

    @Override
    protected AzureBlobStorage createStorageBackend() {
        return new AzureBlobStorage();
    }

    @Override
    protected Map<String, Object> storageConfigForAuthenticatedProxy() {
        final var proxy = PROXY_AUTHENTICATED;
        return Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT),
            "proxy.username", SOCKS5_USER,
            "proxy.password", SOCKS5_PASSWORD
        );
    }

    @Override
    protected Map<String, Object> storageConfigForUnauthenticatedProxy() {
        final var proxy = PROXY_UNAUTHENTICATED;
        return Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT)
        );
    }

    @Override
    protected Map<String, Object> storageConfigForNoProxy() {
        return Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString
        );
    }

    @Override
    protected Iterable<String> possibleRootCauseMessagesWhenNoProxy() {
        // Either - or, it seems it depends on the JVM version.
        final String possibleMessage1 = String.format(
            "%s: Temporary failure in name resolution", azuriteContainerNetworkAlias);
        final String possibleMessage2 = String.format("%s: Name or service not known", azuriteContainerNetworkAlias);
        return List.of(possibleMessage1, possibleMessage2);
    }
}
