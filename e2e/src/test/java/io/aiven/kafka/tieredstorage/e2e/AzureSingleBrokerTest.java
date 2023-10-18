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

package io.aiven.kafka.tieredstorage.e2e;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class AzureSingleBrokerTest extends SingleBrokerTest {
    private static final int BLOB_STORAGE_PORT = 10000;
    static final String NETWORK_ALIAS = "blob-storage";

    static final GenericContainer<?> AZURITE_SERVER =
        new GenericContainer<>(DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite"))
            .withExposedPorts(BLOB_STORAGE_PORT)
            .withCommand("azurite-blob --blobHost 0.0.0.0")
            .withNetwork(NETWORK)
            .withNetworkAliases(NETWORK_ALIAS);

    static final String AZURE_CONTAINER = "test-container";

    static BlobContainerClient blobContainerClient;

    @BeforeAll
    static void init() throws Exception {
        AZURITE_SERVER.start();

        // The well-known Azurite account name and key.
        final String accountName = "devstoreaccount1";
        final String accountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        final String endpointForTestCode =
            "http://127.0.0.1:" + AZURITE_SERVER.getMappedPort(BLOB_STORAGE_PORT) + "/devstoreaccount1";
        final String connectionString = "DefaultEndpointsProtocol=http;"
            + "AccountName=" + accountName + ";"
            + "AccountKey=" + accountKey + ";"
            + "BlobEndpoint=" + endpointForTestCode + ";";
        final var blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildClient();
        blobContainerClient = blobServiceClient.createBlobContainer(AZURE_CONTAINER);

        final String endpointForKafka = "http://" + NETWORK_ALIAS + ":" + BLOB_STORAGE_PORT + "/devstoreaccount1";
        setupKafka(kafka -> kafka.withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.azure.AzureBlobStorage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/azure/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AZURE_CONTAINER_NAME", AZURE_CONTAINER)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AZURE_ACCOUNT_NAME", accountName)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AZURE_ACCOUNT_KEY", accountKey)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AZURE_ENDPOINT_URL", endpointForKafka)
            .dependsOn(AZURITE_SERVER));
    }

    @AfterAll
    static void cleanup() {
        stopKafka();

        AZURITE_SERVER.stop();

        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());

        final var list = blobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null);
        return list.stream().findAny().isEmpty();
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        return blobContainerClient.listBlobs().stream()
            .map(BlobItem::getName)
            .map(k -> k.substring(k.lastIndexOf('/') + 1))
            .sorted()
            .collect(Collectors.toList());
    }
}
