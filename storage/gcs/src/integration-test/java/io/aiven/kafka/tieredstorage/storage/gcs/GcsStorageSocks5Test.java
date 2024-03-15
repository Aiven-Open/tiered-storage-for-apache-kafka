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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.util.List;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseSocks5Test;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class GcsStorageSocks5Test extends BaseSocks5Test<GcsStorage> {
    static final Network NETWORK = Network.newNetwork();

    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer()
        .withNetwork(NETWORK);

    static String fakeGcsContainerNetworkAlias = GCS_SERVER.getNetworkAliases().get(0);

    static String internalGcsEndpoint;

    static {
        internalGcsEndpoint = String.format("http://%s:%d", fakeGcsContainerNetworkAlias, GCS_SERVER.PORT);
        GCS_SERVER.withExternalURL(internalGcsEndpoint);
    }

    @Container
    static final GenericContainer<?> PROXY_AUTHENTICATED = proxyContainer(true).withNetwork(NETWORK);
    @Container
    static final GenericContainer<?> PROXY_UNAUTHENTICATED = proxyContainer(false).withNetwork(NETWORK);

    static Storage storage;

    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        storage = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = testInfo.getDisplayName()
            .toLowerCase()
            .replace("(", "")
            .replace(")", "");
        while (bucketName.length() < 3) {
            bucketName += bucketName;
        }
        storage.create(BucketInfo.newBuilder(bucketName).build());
    }

    @Override
    protected GcsStorage createStorageBackend() {
        return new GcsStorage();
    }

    @Override
    protected Map<String, Object> storageConfigForAuthenticatedProxy() {
        final var proxy = PROXY_AUTHENTICATED;
        return Map.of(
            "gcs.endpoint.url", internalGcsEndpoint,
            "gcs.bucket.name", bucketName,
            "gcs.credentials.default", "false",
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
            "gcs.endpoint.url", internalGcsEndpoint,
            "gcs.bucket.name", bucketName,
            "gcs.credentials.default", "false",
            "proxy.host", proxy.getHost(),
            "proxy.port", proxy.getMappedPort(SOCKS5_PORT)
        );
    }

    @Override
    protected Map<String, Object> storageConfigForNoProxy() {
        return Map.of(
            "gcs.endpoint.url", internalGcsEndpoint,
            "gcs.bucket.name", bucketName,
            "gcs.credentials.default", "false"
        );
    }

    @Override
    protected Iterable<String> possibleRootCauseMessagesWhenNoProxy() {
        return List.of(fakeGcsContainerNetworkAlias);
    }
}
