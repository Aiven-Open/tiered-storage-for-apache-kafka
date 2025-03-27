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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AzureBlobStorageConfigTest {
    private static final String ACCOUNT_NAME = "account1";
    private static final String ACCOUNT_KEY = "account_key";
    private static final String SAS_TOKEN = "token";
    private static final String CONTAINER_NAME = "c1";
    private static final String ENDPOINT = "http://localhost:10000/";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=http;"
        + "AccountName=devstoreaccount1;"
        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        + "BlobEndpoint=http://localhost:10000/devstoreaccount1;";

    @Test
    void minimalConfig() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.container.name", CONTAINER_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.containerName()).isEqualTo(CONTAINER_NAME);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isNull();
        assertThat(config.uploadBlockSize()).isEqualTo(25 * 1024 * 1024);
    }

    @Test
    void shouldRequireContainerName() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"azure.container.name\" which has no default value.");
    }

    @Test
    void shouldRequireAccountNameOrSasTokenIfNoConnectionString() {
        final var configs = Map.of(
            "azure.container.name", CONTAINER_NAME
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("\"azure.account.name\" and/or \"azure.sas.token\" "
                + "must be set if \"azure.connection.string\" is not set.");
    }

    @Test
    void authAccountName() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.container.name", CONTAINER_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void uploadBlockSize() {
        final var size = 1024 * 1024 * 1024;
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.container.name", CONTAINER_NAME,
            "azure.upload.block.size", Integer.toString(size)
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.uploadBlockSize()).isEqualTo(size);
    }

    @Test
    void invalidUploadBlockSize() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.container.name", CONTAINER_NAME,
            "azure.upload.block.size", "100"
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 100 for configuration azure.upload.block.size: Value must be at least 102400");
    }

    @Test
    void authAccountNameAndEndpoint() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.container.name", CONTAINER_NAME,
            "azure.endpoint.url", ENDPOINT
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isEqualTo(ENDPOINT);
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authAccountNameAndKey() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.account.key", ACCOUNT_KEY,
            "azure.container.name", CONTAINER_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isEqualTo(ACCOUNT_KEY);
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authAccountNameAndKeyAndEndpoint() {
        final var configs = Map.of(
            "azure.account.name", ACCOUNT_NAME,
            "azure.account.key", ACCOUNT_KEY,
            "azure.container.name", CONTAINER_NAME,
            "azure.endpoint.url", ENDPOINT
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isEqualTo(ACCOUNT_KEY);
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isEqualTo(ENDPOINT);
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authSasToken() {
        final var configs = Map.of(
            "azure.sas.token", SAS_TOKEN,
            "azure.container.name", CONTAINER_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isNull();
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isEqualTo(SAS_TOKEN);
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authSasTokenAndEndpoint() {
        final var configs = Map.of(
            "azure.sas.token", SAS_TOKEN,
            "azure.container.name", CONTAINER_NAME,
            "azure.endpoint.url", ENDPOINT
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isNull();
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isEqualTo(SAS_TOKEN);
        assertThat(config.endpointUrl()).isEqualTo(ENDPOINT);
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authSasTokenAndAccountName() {
        final var configs = Map.of(
            "azure.sas.token", SAS_TOKEN,
            "azure.container.name", CONTAINER_NAME,
            "azure.account.name", ACCOUNT_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isEqualTo(ACCOUNT_NAME);
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isEqualTo(SAS_TOKEN);
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isNull();
    }

    @Test
    void authConnectionString() {
        final var configs = Map.of(
            "azure.connection.string", CONNECTION_STRING,
            "azure.container.name", CONTAINER_NAME
        );
        final var config = new AzureBlobStorageConfig(configs);
        assertThat(config.accountName()).isNull();
        assertThat(config.accountKey()).isNull();
        assertThat(config.sasToken()).isNull();
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.connectionString()).isEqualTo(CONNECTION_STRING);
    }

    @Test
    void connectionStringAndAccountNameClash() {
        final var configs = Map.of(
            "azure.connection.string", CONNECTION_STRING,
            "azure.container.name", CONTAINER_NAME,
            "azure.account.name", ACCOUNT_NAME
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("\"azure.connection.string\" cannot be set together with \"azure.account.name\".");
    }

    @Test
    void connectionStringAndAccountKeyClash() {
        final var configs = Map.of(
            "azure.connection.string", CONNECTION_STRING,
            "azure.container.name", CONTAINER_NAME,
            "azure.account.key", ACCOUNT_KEY
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("\"azure.connection.string\" cannot be set together with \"azure.account.key\".");
    }

    @Test
    void connectionStringAndSasTokenClash() {
        final var configs = Map.of(
            "azure.connection.string", CONNECTION_STRING,
            "azure.container.name", CONTAINER_NAME,
            "azure.sas.token", SAS_TOKEN
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("\"azure.connection.string\" cannot be set together with \"azure.sas.token\".");
    }

    @Test
    void connectionStringAndEndpointClash() {
        final var configs = Map.of(
            "azure.connection.string", CONNECTION_STRING,
            "azure.container.name", CONTAINER_NAME,
            "azure.endpoint.url", ENDPOINT
        );
        assertThatThrownBy(() -> new AzureBlobStorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("\"azure.connection.string\" cannot be set together with \"azure.endpoint.url\".");
    }
}
