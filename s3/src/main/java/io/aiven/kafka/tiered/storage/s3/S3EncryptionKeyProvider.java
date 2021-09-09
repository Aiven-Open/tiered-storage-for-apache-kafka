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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;

import io.aiven.kafka.tiered.storage.commons.io.IOUtils;
import io.aiven.kafka.tiered.storage.commons.metadata.EncryptedRepositoryMetadata;
import io.aiven.kafka.tiered.storage.commons.security.EncryptionKeyProvider;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3EncryptionKeyProvider {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);
    private static final String SEGMENT_METADATA_FILE_NAME = ".metadata.json";
    private final AmazonS3 s3Client;
    private final S3RemoteStorageManagerConfig config;
    private final EncryptionKeyProvider encryptionKeyProvider;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public S3EncryptionKeyProvider(final AmazonS3 s3Client, final S3RemoteStorageManagerConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        final String publicKey = config.publicKey();
        final String privateKey = config.privateKey();
        encryptionKeyProvider = EncryptionKeyProvider.of(
                new ByteArrayInputStream(publicKey.getBytes(StandardCharsets.UTF_8)),
                new ByteArrayInputStream(privateKey.getBytes(StandardCharsets.UTF_8))
        );
    }

    public SecretKey createOrRestoreEncryptionKey(final String metadataFileKey) {
        final EncryptedRepositoryMetadata repositoryMetadata =
                new EncryptedRepositoryMetadata(encryptionKeyProvider);
        if (s3Client.doesObjectExist(config.s3BucketName(), metadataFileKey)) {
            log.info("Restore encryption key for repository. Path: {}", metadataFileKey);
            try (final InputStream in = s3Client.getObject(config.s3BucketName(),
                    metadataFileKey).getObjectContent()) {
                return repositoryMetadata.deserialize(in.readAllBytes());
            } catch (final AmazonClientException | IOException e) {
                throw new RuntimeException("Couldn't read metadata file from bucket " + config.s3BucketName(), e);
            }
        } else {
            log.info("Create new encryption key for repository. Path: {}", SEGMENT_METADATA_FILE_NAME);
            final SecretKey encryptionKey = encryptionKeyProvider.createKey();
            try {
                uploadMetadata(repositoryMetadata.serialize(encryptionKey), metadataFileKey);
            } catch (final IOException e) {
                throw new RuntimeException("Failed to create new metadata file", e);
            }
            return encryptionKey;
        }
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
