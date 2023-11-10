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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class AzuriteBlobStorageUtils {
    static GenericContainer<?> azuriteContainer(final int port) {
        return
            new GenericContainer<>(DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite"))
                .withCopyFileToContainer(
                    MountableFile.forClasspathResource("/azurite-cert.pem"),
                    "/opt/azurite/azurite-cert.pem")
                .withCopyFileToContainer(
                    MountableFile.forClasspathResource("/azurite-key.pem"),
                    "/opt/azurite/azurite-key.pem")
                .withExposedPorts(port)
                .withCommand("azurite-blob --blobHost 0.0.0.0 "
                    + "--cert /opt/azurite/azurite-cert.pem --key /opt/azurite/azurite-key.pem");
    }


    static String endpoint(final GenericContainer<?> azuriteContainer, final int port) {
        return "https://127.0.0.1:" + azuriteContainer.getMappedPort(port) + "/devstoreaccount1";
    }

    static String connectionString(final GenericContainer<?> azuriteContainer, final int port) {
        // The well-known Azurite connection string.
        return "DefaultEndpointsProtocol=https;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=" + endpoint(azuriteContainer, port) + ";";
    }
}
