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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.azuriteContainer;
import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.connectionString;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Testcontainers
class AzureBlobStorageSocks5Test {
    protected static final int SOCKS5_PORT = 1080;
    protected static final String SOCKS5_USER = "user";
    protected static final String SOCKS5_PASSWORD = "password";

    protected static final ObjectKey OBJECT_KEY = () -> "aaa";
    protected static final byte[] TEST_DATA = {0, 1, 2, 3};

    static final Network NETWORK = Network.newNetwork();

    static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT)
        .withNetwork(NETWORK);

    @Container
    static final GenericContainer<?> PROXY_AUTHENTICATED =
        new GenericContainer<>(DockerImageName.parse("ananclub/ss5"))
            .withEnv(Map.of(
                "LISTEN", "0.0.0.0:" + SOCKS5_PORT,
                "USER", SOCKS5_USER,
                "PASSWORD", SOCKS5_PASSWORD
            ))
            .withExposedPorts(SOCKS5_PORT)
            .withNetwork(NETWORK);

    @Container
    static final GenericContainer<?> PROXY_UNAUTHENTICATED =
        new GenericContainer<>(DockerImageName.parse("ananclub/ss5"))
            .withEnv(Map.of(
                "LISTEN", "0.0.0.0:" + SOCKS5_PORT
            ))
            .withExposedPorts(SOCKS5_PORT)
            .withNetwork(NETWORK);

    static BlobServiceClient blobServiceClient;

    static String azuriteContainerNetworkAlias = AZURITE_SERVER.getNetworkAliases().get(0);

    // This endpoint points to Azurite with the Docker-internal hostname and port.
    // The host running these tests cannot reach it directly, only via a proxy.
    static String internalConnectionString;

    protected String azureContainerName;

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
        azureContainerName = testInfo.getDisplayName()
            .toLowerCase()
            .replace("(", "")
            .replace(")", "");
        while (azureContainerName.length() < 3) {
            azureContainerName += azureContainerName;
        }
        blobServiceClient.createBlobContainer(azureContainerName);
    }

    @Test
    void worksWithAuthenticatedProxy() throws StorageBackendException, IOException {
        // Ensure we can access the storage through the proxy with authentication.
        final var proxy = PROXY_AUTHENTICATED;
        final AzureBlobStorage storage = new AzureBlobStorage();
        storage.configure(Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT),
            "proxy.username", SOCKS5_USER,
            "proxy.password", SOCKS5_PASSWORD
        ));

        storage.upload(new ByteArrayInputStream(TEST_DATA), OBJECT_KEY);
        assertThat(storage.fetch(OBJECT_KEY).readAllBytes()).isEqualTo(TEST_DATA);
    }

    @Test
    void worksWithUnauthenticatedProxy() throws StorageBackendException, IOException {
        // Ensure we can access the storage through the proxy without authentication.
        final var proxy = PROXY_UNAUTHENTICATED;
        final AzureBlobStorage storage = new AzureBlobStorage();
        storage.configure(Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString,
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT)
        ));

        storage.upload(new ByteArrayInputStream(TEST_DATA), OBJECT_KEY);
        assertThat(storage.fetch(OBJECT_KEY).readAllBytes()).isEqualTo(TEST_DATA);
    }

    @Test
    void doesNotWorkWithoutProxy() {
        // This test accompanies the other ones by ensuring that _without_ a proxy
        // we cannot even resolve the host name of the server, which is internal to the Docker network.

        // Due to the nature to the Java name resolution and/or getaddrinfo,
        // this has a pretty long unconfigurable timeout, so the test may take a couple of minutes.

        final AzureBlobStorage storage = new AzureBlobStorage();
        storage.configure(Map.of(
            "azure.container.name", azureContainerName,
            "azure.connection.string", internalConnectionString
        ));

        // Either - or, it seems it depends on the JVM version.
        final String possibleMessage1 = String.format(
            "%s: Temporary failure in name resolution", azuriteContainerNetworkAlias);
        final String possibleMessage2 = String.format("%s: Name or service not known", azuriteContainerNetworkAlias);
        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(new byte[] {0, 1, 2, 3}), OBJECT_KEY))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasRootCauseInstanceOf(UnknownHostException.class)
            .rootCause().message()
            .isIn(possibleMessage1, possibleMessage2);
        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasRootCauseInstanceOf(UnknownHostException.class)
            .rootCause().message()
            .isIn(possibleMessage1, possibleMessage2);
    }
}
