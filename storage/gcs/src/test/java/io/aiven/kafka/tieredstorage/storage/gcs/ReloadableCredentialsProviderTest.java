/*
 * Copyright 2025 Aiven Oy
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReloadableCredentialsProviderTest {

    private static final String VALID_CREDENTIALS_JSON = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"test-project\",\n"
        + "  \"private_key_id\": \"test-key-id\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
        + "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAwyPCXjWv30y+ZGJH\\njKGsIem4OlXEwsgsl6bJr0vKga/GYEVZXsKz/1U"
        + "vKArCQNLOfJh/CpUE+cSLn+H7\\ngZ1uSwIDAQABAkAF1H2sHuKAQ2S0zxLgKrxfzwHIDGPyhdR/O2ZvLE6CjVZ0J4PD\\n+Gt3nJQUcEL"
        + "CEjc3y3RnlOsGd7TTPsZHP7CRAiEA8f75YoDbDcPpd6SK4/PoWmTD\\nBBprsvsQbWL5Vpx0AH8CIQDObqMNKTCtz64tDULI0JSECu7Rni"
        + "RFyQCQ6H/ZMLys\\nNQIgM68eOjCFGGqIOXpWA5t7O5sbn4u5Bs/iUUp7MElX6ScCIHJBOAvDvYamCOA0\\nk78z+s9ugaoRXkAltSN/G6"
        + "vpVrP1AiBhNDs+MZSYh92/A8j/GC/I8yvlkOSFo/ME\\n/Va0X/P2Ng==\\n-----END PRIVATE KEY-----\\n\",\n"
        + "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n"
        + "  \"client_id\": \"123456789012345678901\",\n"
        + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
        + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
        + "}";

    private static final String UPDATED_CREDENTIALS_JSON = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"updated-project\",\n"
        + "  \"private_key_id\": \"updated-key-id\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
        + "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAwyPCXjWv30y+ZGJH\\njKGsIem4OlXEwsgsl6bJr0vKga/GYEVZXsKz/1Uv"
        + "KArCQNLOfJh/CpUE+cSLn+H7\\ngZ1uSwIDAQABAkAF1H2sHuKAQ2S0zxLgKrxfzwHIDGPyhdR/O2ZvLE6CjVZ0J4PD\\n+Gt3nJQUcELCEj"
        + "c3y3RnlOsGd7TTPsZHP7CRAiEA8f75YoDbDcPpd6SK4/PoWmTD\\nBBprsvsQbWL5Vpx0AH8CIQDObqMNKTCtz64tDULI0JSECu7RniRFyQ"
        + "CQ6H/ZMLys\\nNQIgM68eOjCFGGqIOXpWA5t7O5sbn4u5Bs/iUUp7MElX6ScCIHJBOAvDvYamCOA0\\nk78z+s9ugaoRXkAltSN/G6vpVrP1"
        + "AiBhNDs+MZSYh92/A8j/GC/I8yvlkOSFo/ME\\n/Va0X/P2Ng==\\n-----END PRIVATE KEY-----\\n\",\n"
        + "  \"client_email\": \"updated@updated-project.iam.gserviceaccount.com\",\n"
        + "  \"client_id\": \"123456789012345678902\",\n"
        + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
        + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
        + "}";

    private static final String ACCESS_TOKEN_CREDENTIALS_JSON = "{\n"
        + "  \"access_token\": \"ya29.a0AfH6SMC...\"\n"
        + "}";

    @Test
    void testJsonCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, VALID_CREDENTIALS_JSON, null, 1)) {

            final Credentials credentials = provider.getCredentials();
            assertNotNull(credentials);
        }
    }

    @Test
    void testNoCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            false, null, null, 1)) {

            final Credentials credentials = provider.getCredentials();
            assertTrue(credentials instanceof NoCredentials);
        }
    }

    @Test
    void testAccessTokenCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, ACCESS_TOKEN_CREDENTIALS_JSON, null)) {

            final Credentials credentials = provider.getCredentials();
            assertNotNull(credentials);
            assertEquals("OAuth2", credentials.getAuthenticationType());
            assertEquals("Bearer ya29.a0AfH6SMC...", credentials.getRequestMetadata().get("Authorization").get(0));
        }
    }

    @Test
    void testPathCredentialsWithAutoReload(@TempDir final Path tempDir) throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString(), 1)) {

            final Credentials credentials = provider.getCredentials();
            assertNotNull(credentials);
        }
    }

    @Test
    void testPathCredentialsWithAutoReloadDetection(@TempDir final Path tempDir)
        throws IOException, InterruptedException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final AtomicInteger callbackCount = new AtomicInteger(0);
        final AtomicReference<Credentials> latestCredentials = new AtomicReference<>();
        final CountDownLatch callbackLatch = new CountDownLatch(1);

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString(), 1)) {

            provider.setCredentialsUpdateCallback(credentials -> {
                callbackCount.incrementAndGet();
                latestCredentials.set(credentials);
                callbackLatch.countDown();
            });

            final Credentials initialCredentials = provider.getCredentials();
            assertNotNull(initialCredentials);

            // Update the credentials file
            Files.write(credentialsFile, UPDATED_CREDENTIALS_JSON.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Wait for the callback to be called
            assertTrue(callbackLatch.await(5, TimeUnit.SECONDS), "Callback should be called within 5 seconds");

            assertEquals(1, callbackCount.get());
            assertNotNull(latestCredentials.get());

            // Update the credentials file again with same semantical contents
            Files.write(credentialsFile, (UPDATED_CREDENTIALS_JSON + " ").getBytes(),
                        StandardOpenOption.TRUNCATE_EXISTING);

            Thread.sleep(2000);
            assertEquals(1, callbackCount.get());
            assertNotNull(latestCredentials.get());
        }
    }

    @Test
    void testFileWatchingWithInvalidUpdate(@TempDir final Path tempDir) throws IOException, InterruptedException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final AtomicInteger successCallbackCount = new AtomicInteger(0);

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString(), 1)) {

            provider.setCredentialsUpdateCallback(credentials -> successCallbackCount.incrementAndGet());

            final Credentials initialCredentials = provider.getCredentials();
            assertNotNull(initialCredentials);

            // Write invalid JSON to trigger an error
            Files.write(credentialsFile, "invalid json".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Give some time for the file watcher to process the change
            Thread.sleep(2000);

            // The callback should not have been called due to the error
            assertEquals(0, successCallbackCount.get());

            // Write valid JSON again
            Files.write(credentialsFile, UPDATED_CREDENTIALS_JSON.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Give some time for the file watcher to process the change
            Thread.sleep(2000);

            // Now the callback should be called
            assertEquals(1, successCallbackCount.get());
        }
    }

    @Test
    void testClose(@TempDir final Path tempDir) throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString(), 1);

        // Should not throw any exception
        provider.close();

        // Should be able to call close multiple times
        provider.close();
    }
}
