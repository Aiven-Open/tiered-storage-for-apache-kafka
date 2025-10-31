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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test demonstrating the full credentials auto-reload functionality
 * in a simulated GCS storage environment.
 */
class GcsCredentialsAutoReloadIntegrationTest {

    private static final String INITIAL_CREDENTIALS_JSON = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"test-project\",\n"
        + "  \"private_key_id\": \"test-key-id\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
        + "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAwyPCXjWv30y+ZGJH\\njKGsIem4OlXEwsgsl6bJr0vKga/GYEVZXsKz/1Uv"
        + "KArCQNLOfJh/CpUE+cSLn+H7\\ngZ1uSwIDAQABAkAF1H2sHuKAQ2S0zxLgKrxfzwHIDGPyhdR/O2ZvLE6CjVZ0J4PD\\n+Gt3nJQUcELCEj"
        + "c3y3RnlOsGd7TTPsZHP7CRAiEA8f75YoDbDcPpd6SK4/PoWmTD\\nBBprsvsQbWL5Vpx0AH8CIQDObqMNKTCtz64tDULI0JSECu7RniRFyQ"
        + "CQ6H/ZMLys\\nNQIgM68eOjCFGGqIOXpWA5t7O5sbn4u5Bs/iUUp7MElX6ScCIHJBOAvDvYamCOA0\\nk78z+s9ugaoRXkAltSN/G6vpVrP1"
        + "AiBhNDs+MZSYh92/A8j/GC/I8yvlkOSFo/ME\\n/Va0X/P2Ng==\\n-----END PRIVATE KEY-----\\n\",\n"
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

    @Test
    void testGcsStorageWithFileBasedCredentialsAutoReload(@TempDir final Path tempDir) throws Exception {
        // Create initial credentials file
        final Path credentialsFile = tempDir.resolve("gcs-credentials.json");
        Files.write(credentialsFile, INITIAL_CREDENTIALS_JSON.getBytes());

        // Configure GCS storage with file-based credentials (auto-reload is automatic)
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.credentials.path", credentialsFile.toString()
        );

        final AtomicInteger credentialsUpdateCount = new AtomicInteger(0);
        final CountDownLatch updateLatch = new CountDownLatch(1);

        // Create a test storage that we can monitor for credential updates
        final TestableGcsStorage testableStorage = new TestableGcsStorage();
        testableStorage.configure(configs);
        testableStorage.setCredentialsUpdateCallback(() -> {
            credentialsUpdateCount.incrementAndGet();
            updateLatch.countDown();
        });

        try {
            // Verify initial setup
            assertEquals(0, credentialsUpdateCount.get());

            // Update the credentials file
            Files.write(credentialsFile, UPDATED_CREDENTIALS_JSON.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Wait for the auto-reload to trigger
            assertTrue(updateLatch.await(11, TimeUnit.SECONDS),
                "Credentials should be reloaded within 15 seconds");

            // Verify that credentials were updated
            assertEquals(1, credentialsUpdateCount.get());

            // Update the credentials file again to see if callback is not called without waiting
            Files.write(credentialsFile, (UPDATED_CREDENTIALS_JSON + " ").getBytes(),
                        StandardOpenOption.TRUNCATE_EXISTING);

            Thread.sleep(11000); // Wait to ensure no further callbacks happen
            assertEquals(1, credentialsUpdateCount.get(), "Credentials should not have been reloaded");

        } finally {
            testableStorage.close();
        }
    }

    @Test
    void testGcsStorageWithJsonCredentialsNoAutoReload(@TempDir final Path tempDir) throws Exception {
        // Configure GCS storage with JSON credentials (no auto-reload for JSON)
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.credentials.json", INITIAL_CREDENTIALS_JSON
        );

        final TestableGcsStorage storage = new TestableGcsStorage();
        final AtomicInteger credentialsUpdateCount = new AtomicInteger(0);

        storage.configure(configs);
        storage.setCredentialsUpdateCallback(() -> credentialsUpdateCount.incrementAndGet());

        try {
            // Verify initial setup
            assertEquals(0, credentialsUpdateCount.get());

            // No file watching should be active for JSON credentials
            // Wait a bit to ensure no callbacks happen
            Thread.sleep(11000);

            // Verify that no automatic credential updates occurred
            assertEquals(0, credentialsUpdateCount.get());

        } finally {
            storage.close();
        }
    }

    /**
     * A testable version of GcsStorage that allows us to monitor credential updates
     * without requiring actual GCS connectivity.
     */
    private static class TestableGcsStorage extends GcsStorage {
        private Runnable credentialsUpdateCallback;

        public void setCredentialsUpdateCallback(final Runnable callback) {
            this.credentialsUpdateCallback = callback;
        }

        @Override
        protected void updateStorageClient(final com.google.auth.Credentials credentials) {
            // Don't actually create GCS storage client in tests
            // Just notify that credentials were updated
            if (credentialsUpdateCallback != null) {
                credentialsUpdateCallback.run();
            }
        }
    }
}
