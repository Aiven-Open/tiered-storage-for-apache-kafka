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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class S3RotatingCredentialsProvider implements AwsCredentialsProvider, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3RotatingCredentialsProvider.class);

    private final File credentialsFile;
    private volatile WatchService watchService;
    private java.util.concurrent.ScheduledExecutorService scheduledExecutorService;
    private volatile AwsCredentials currentCredentials;

    S3RotatingCredentialsProvider(final String credentialsFile) {
        this.credentialsFile = new File(credentialsFile);
        try {
            this.currentCredentials = loadCredentialsFromFile();
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to load initial credentials from file: " + credentialsFile, e);
        }

        if (currentCredentials == null) {
            throw new IllegalArgumentException("Failed to load initial credentials from file: " + credentialsFile);
        }

        this.watchService = initializeWatchService();

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "s3-credentials-watcher");
            t.setDaemon(true);
            return t;
        });

        final AtomicReference<WatchKey> watchKeyReference = new AtomicReference<>(subscribeToCredentialChanges());
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            final WatchKey watchKey = watchKeyReference.get();
            if (watchKey != null) {
                final List<WatchEvent<?>> watchEvents = watchKey.pollEvents();

                watchEvents.stream().filter(watchEvent -> {
                    @SuppressWarnings("unchecked")
                    final Path path = ((WatchEvent<Path>) watchEvent).context();
                    return this.credentialsFile.toPath().getFileName().equals(path);
                }).findFirst().ifPresent(watchEvent -> {
                    LOGGER.info("{}: {}, Modified: {}",
                            watchEvent.kind(), watchEvent.context(), this.credentialsFile.lastModified());
                    try {
                        final var newCredentials = loadCredentialsFromFile();
                        if (newCredentials != null) {
                            currentCredentials = newCredentials;
                            LOGGER.info("Credentials updated");
                        }
                    } catch (final Exception e) {
                        LOGGER.error("Failed to reload credentials from file", e);
                    }
                });
                if (!watchKey.reset()) {
                    watchKeyReference.compareAndSet(watchKey, subscribeToCredentialChanges());
                }
            } else {
                watchKeyReference.set(subscribeToCredentialChanges());
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (watchService != null) {
            try {
                watchService.close();
            } catch (final IOException e) {
                LOGGER.error("Failed to stop watch service", e);
            }
        }
    }

    private AwsCredentials loadCredentialsFromFile() throws IOException {
        final String rsmConfigStoragePrefix = "rsm.config.storage.";
        final Properties props = Utils.loadProps(credentialsFile.getAbsolutePath(),
                Arrays.asList(S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG,
                        rsmConfigStoragePrefix + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG,
                        S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG,
                        rsmConfigStoragePrefix + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG,
                        S3StorageConfig.AWS_SESSION_TOKEN_CONFIG,
                        rsmConfigStoragePrefix + S3StorageConfig.AWS_SESSION_TOKEN_CONFIG));
        final String accessKey =
            props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG) != null
                ? props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG)
                : props.getProperty(S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG);
        final String secretKey =
            props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG) != null
                ? props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG)
                : props.getProperty(S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG);
        final String sessionToken =
            props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_SESSION_TOKEN_CONFIG) != null
                ? props.getProperty(rsmConfigStoragePrefix + S3StorageConfig.AWS_SESSION_TOKEN_CONFIG)
                : props.getProperty(S3StorageConfig.AWS_SESSION_TOKEN_CONFIG);

        if (accessKey != null && secretKey != null) {
            if (sessionToken != null) {
                return AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
            } else {
                return AwsBasicCredentials.create(accessKey, secretKey);
            }
        }
        return currentCredentials;
    }

    private WatchService initializeWatchService() {
        try {
            return FileSystems.getDefault().newWatchService();
        } catch (final IOException e) {
            LOGGER.error("Failed to initialize WatchService", e);
            throw new RuntimeException(e);
        }
    }

    private WatchKey subscribeToCredentialChanges() {
        try {
            return credentialsFile.toPath().toAbsolutePath().getParent()
                    .register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
                            StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
        } catch (final IOException e) {
            LOGGER.error("Failed to subscribe to credential changes", e);
            return null;
        }
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return currentCredentials;
    }
}
