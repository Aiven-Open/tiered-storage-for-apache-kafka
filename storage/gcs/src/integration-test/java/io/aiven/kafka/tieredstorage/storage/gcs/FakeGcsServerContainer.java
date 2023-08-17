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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.testcontainers.containers.GenericContainer;

class FakeGcsServerContainer extends GenericContainer<FakeGcsServerContainer> {
    private static final int PORT = 4443;

    private static final String VERSION = "1.47.4";

    FakeGcsServerContainer() {
        super("fsouza/fake-gcs-server:" + VERSION);

        withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
            "/bin/fake-gcs-server",
            "-scheme", "http"
        ));
        addExposedPorts(PORT);
    }

    @Override
    public void start() {
        super.start();
        updateExternalUrl();
    }

    String getURL() {
        return "http://" + getHost() + ":" + getMappedPort(PORT);
    }

    private void updateExternalUrl() {
        final String modifyExternalUrlRequestUri = getURL() + "/_internal/config";
        final String updateExternalUrlJson = "{"
            + "\"externalUrl\": \"" + getURL() + "\""
            + "}";

        final HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(modifyExternalUrlRequestUri))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(updateExternalUrlJson))
            .build();
        final HttpResponse<Void> response;
        try {
            response = HttpClient.newBuilder().build()
                .send(req, HttpResponse.BodyHandlers.discarding());
        } catch (final InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "error updating fake-gcs-server with external url, response status code "
                    + response.statusCode() + " != 200");
        }
    }
}
