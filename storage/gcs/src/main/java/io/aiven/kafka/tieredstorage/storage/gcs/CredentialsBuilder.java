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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;

final class CredentialsBuilder {
    private CredentialsBuilder() {
        // hide constructor
    }

    /**
     * Builds {@link GoogleCredentials}.
     *
     * <p>{@code credentialsPath} and {@code credentialsJson} are mutually exclusive: if both are provided (are
     * non-{@code null}), this is an error. They are also cannot be set together with
     * {@code defaultCredentials == true}.
     *
     * <p>If either {@code credentialsPath} or {@code credentialsJson} is provided, it's used to
     * construct the credentials.
     *
     * <p>If none are provided, the default GCP SDK credentials acquisition mechanism is used
     * or the no-credentials object is returned.
     *
     * @param defaultCredentials use the default credentials.
     * @param credentialsJson the credential JSON string, can be {@code null}.
     * @param credentialsPath the credential path, can be {@code null}.
     * @return a {@link GoogleCredentials} constructed based on the input.
     * @throws IOException              if some error getting the credentials happen.
     * @throws IllegalArgumentException if a combination of parameters is invalid.
     */
    public static Credentials build(final boolean defaultCredentials,
                                    final String credentialsJson,
                                    final String credentialsPath)
        throws IOException {
        if (defaultCredentials && credentialsJson != null) {
            throw new IllegalArgumentException(
                "defaultCredentials == true and credentialsJson != null cannot be simultaneously.");
        }
        if (defaultCredentials && credentialsPath != null) {
            throw new IllegalArgumentException(
                "defaultCredentials == true and credentialsPath != null cannot be simultaneously.");
        }
        if (credentialsJson != null && credentialsPath != null) {
            throw new IllegalArgumentException(
                "Both credentialsPath and credentialsJson cannot be non-null.");
        }

        if (defaultCredentials) {
            return GoogleCredentials.getApplicationDefault();
        }

        if (credentialsJson != null) {
            return getCredentialsFromJson(credentialsJson);
        }

        if (credentialsPath != null) {
            return getCredentialsFromPath(credentialsPath);
        }

        return NoCredentials.getInstance();
    }

    private static GoogleCredentials getCredentialsFromPath(final String credentialsPath) throws IOException {
        try (final InputStream stream = Files.newInputStream(Paths.get(credentialsPath))) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read GCS credentials from " + credentialsPath, e);
        }
    }

    private static GoogleCredentials getCredentialsFromJson(final String credentialsJson) throws IOException {
        try (final InputStream stream = new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8))) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read credentials from JSON string", e);
        }
    }
}
