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
import java.util.Objects;
import java.util.stream.Stream;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;

final class CredentialsBuilder {
    private CredentialsBuilder() {
        // hide constructor
    }

    public static void validate(final Boolean defaultCredentials,
                                final String credentialsJson,
                                final String credentialsPath) {
        final long nonNulls = Stream.of(defaultCredentials, credentialsJson, credentialsPath)
            .filter(Objects::nonNull).count();
        if (nonNulls == 0) {
            throw new IllegalArgumentException(
                "All defaultCredentials, credentialsJson, and credentialsPath cannot be null simultaneously.");
        }
        if (nonNulls > 1) {
            throw new IllegalArgumentException(
                "Only one of defaultCredentials, credentialsJson, and credentialsPath can be non-null.");
        }
    }

    /**
     * Builds {@link GoogleCredentials}.
     *
     * <p>{@code credentialsPath}, {@code credentialsJson}, and {@code defaultCredentials true}
     * are mutually exclusive: if more than one are provided (are non-{@code null}), this is an error.
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
    public static Credentials build(final Boolean defaultCredentials,
                                    final String credentialsJson,
                                    final String credentialsPath)
        throws IOException {
        validate(defaultCredentials, credentialsJson, credentialsPath);

        if (credentialsJson != null) {
            return getCredentialsFromJson(credentialsJson);
        }

        if (credentialsPath != null) {
            return getCredentialsFromPath(credentialsPath);
        }

        if (Boolean.TRUE.equals(defaultCredentials)) {
            return GoogleCredentials.getApplicationDefault();
        } else {
            return NoCredentials.getInstance();
        }
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
