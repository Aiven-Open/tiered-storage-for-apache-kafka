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
import java.nio.charset.StandardCharsets;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CredentialsBuilderTest {
    @Test
    void defaultAndJsonProvided() {
        assertThatThrownBy(() -> CredentialsBuilder.build(true, "{}", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("defaultCredentials == true and credentialsJson != null cannot be simultaneously.");
    }

    @Test
    void defaultAndPathProvided() {
        assertThatThrownBy(() -> CredentialsBuilder.build(true, null, "file.json"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("defaultCredentials == true and credentialsPath != null cannot be simultaneously.");
    }

    @Test
    void bothCredentialsProvided() {
        assertThatThrownBy(() -> CredentialsBuilder.build(false, "{}", "file.json"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Both credentialsPath and credentialsJson cannot be non-null.");
    }

    @Test
    void defaultCredentials() throws IOException {
        final GoogleCredentials mockCredentials = GoogleCredentials.newBuilder().build();
        try (final MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
                 Mockito.mockStatic(GoogleCredentials.class)) {
            googleCredentialsMockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
            final Credentials r = CredentialsBuilder.build(true, null, null);
            assertThat(r).isSameAs(mockCredentials);
        }
    }

    @Test
    void noCredentials() throws IOException {
        final Credentials r = CredentialsBuilder.build(false, null, null);
        assertThat(r).isSameAs(NoCredentials.getInstance());
    }

    @Test
    void fromJson() throws IOException {
        final String credentialsJson = Resources.toString(
            Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json"),
            StandardCharsets.UTF_8);
        final Credentials credentials = CredentialsBuilder.build(false, credentialsJson, null);
        assertCredentials(credentials);
    }

    @Test
    void fromPath() throws IOException {
        final String credentialsPath = Thread.currentThread()
            .getContextClassLoader()
            .getResource("test_gcs_credentials.json")
            .getPath();
        final Credentials credentials = CredentialsBuilder.build(false, null, credentialsPath);
        assertCredentials(credentials);
    }

    private static void assertCredentials(final Credentials credentials) {
        assertThat(credentials).isInstanceOf(UserCredentials.class);
        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertThat(userCredentials.getClientId()).isEqualTo("test-client-id");
        assertThat(userCredentials.getClientSecret()).isEqualTo("test-client-secret");
        assertThat(userCredentials.getRefreshToken()).isEqualTo("x");
    }
}
