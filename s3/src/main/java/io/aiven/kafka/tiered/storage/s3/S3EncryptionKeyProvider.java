/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tiered.storage.s3;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.Security;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.commons.io.IOUtils;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.security.metadata.EncryptedRepositoryMetadata;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3EncryptionKeyProvider {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);
    private final AmazonS3 s3Client;
    private final S3RemoteStorageManagerConfig config;
    private final EncryptionProvider encryptionKeyProvider;
    private final Cache<String, byte[]> encryptionMetadataCache;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public S3EncryptionKeyProvider(final AmazonS3 s3Client, final S3RemoteStorageManagerConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        this.encryptionMetadataCache = Caffeine.newBuilder()
                .expireAfterWrite(config.encryptionMetadataCacheRetentionMs(), TimeUnit.MILLISECONDS)
                .maximumSize(config.encryptionMetadataCacheSize())
                .build();
        encryptionKeyProvider = EncryptionProvider.of(Path.of(config.publicKey()), Path.of(config.privateKey()));
    }

    public SecretKey createOrRestoreEncryptionKey(final String metadataFileKey) {
        final byte[] secretKey = createOrRestoreEncryptionMetadata(metadataFileKey);
        final byte[] encryptionKey = new byte[32];
        System.arraycopy(secretKey, 0, encryptionKey, 0, 32);
        return new SecretKeySpec(encryptionKey, "AES");
    }

    public byte[] createOrRestoreAAD(final String metadataFileKey) {
        final byte[] secretKey = createOrRestoreEncryptionMetadata(metadataFileKey);
        final byte[] aad = new byte[32];
        System.arraycopy(secretKey, 32, aad, 0, 32);
        return aad;
    }

    private byte[] createOrRestoreEncryptionMetadata(final String metadataFileKey) {
        final EncryptedRepositoryMetadata repositoryMetadata =
                new EncryptedRepositoryMetadata(encryptionKeyProvider);
        return encryptionMetadataCache.get(metadataFileKey, key -> {
            if (s3Client.doesObjectExist(config.s3BucketName(), metadataFileKey)) {
                log.info("Restoring encryption key from metadata file. Path: {}", metadataFileKey);
                try (final InputStream in = s3Client.getObject(config.s3BucketName(),
                        metadataFileKey).getObjectContent()) {
                    return repositoryMetadata.deserialize(in.readAllBytes()).getEncoded();
                } catch (final AmazonClientException | IOException e) {
                    throw new RuntimeException("Couldn't read metadata file from bucket " + config.s3BucketName(), e);
                }
            } else {
                log.info("Creating new metadata file. Path: {}", metadataFileKey);
                final SecretKey dataKey = encryptionKeyProvider.createDataKeyAndAAD().dataKey;
                try {
                    uploadMetadata(repositoryMetadata.serialize(dataKey), metadataFileKey);
                } catch (final IOException e) {
                    throw new RuntimeException("Failed to create new metadata file", e);
                }
                return dataKey.getEncoded();
            }
        });
    }

    private void uploadMetadata(final byte[] repositoryMetadata, final String metadataFileKey)
            throws IOException {
        try (final ByteArrayInputStream in = new ByteArrayInputStream(repositoryMetadata);
             final S3OutputStream out =
                     new S3OutputStream(config.s3BucketName(),
                             metadataFileKey,
                             config.multiPartUploadPartSize(),
                             s3Client)) {
            IOUtils.copy(in, out, config.ioBufferSize());
        }
    }
}
