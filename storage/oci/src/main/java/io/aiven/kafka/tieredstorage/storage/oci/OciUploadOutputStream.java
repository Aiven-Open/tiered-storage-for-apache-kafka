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

package io.aiven.kafka.tieredstorage.storage.oci;

import java.io.InputStream;
import java.util.List;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.upload.AbstractUploadOutputStream;

import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadPartDetails;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.AbortMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OciUploadOutputStream extends AbstractUploadOutputStream<CommitMultipartUploadPartDetails> {

    private static final Logger log = LoggerFactory.getLogger(OciUploadOutputStream.class);

    private final ObjectStorageClient client;
    private final String namespaceName;
    private final StorageTier storageTier;

    public OciUploadOutputStream(final String namespaceName,
                                 final String bucketName,
                                 final ObjectKey key,
                                 final int partSize,
                                 final StorageTier storageTier,
                                 final ObjectStorageClient client) {
        super(bucketName, key.value(), partSize);
        this.namespaceName = namespaceName;
        this.storageTier = storageTier;
        this.client = client;
    }

    @Override
    protected String createMultipartUploadRequest(final String bucketName, final String key) {
        final CreateMultipartUploadDetails.Builder builder = CreateMultipartUploadDetails.builder();
        if (storageTierDefined()) {
            builder.storageTier(storageTier);
        }
        final CreateMultipartUploadDetails createDetails = builder
                .object(key)
                .contentType("application/octet-stream")
                .build();

        final CreateMultipartUploadRequest initialRequest = CreateMultipartUploadRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .createMultipartUploadDetails(createDetails)
                .build();
        final CreateMultipartUploadResponse response = client.createMultipartUpload(initialRequest);
        final String uploadId = response.getMultipartUpload().getUploadId();
        log.debug("Create new multipart upload request: {}", uploadId);
        return uploadId;
    }

    private boolean storageTierDefined() {
        return storageTier != null && storageTier != StorageTier.UnknownEnumValue;
    }

    protected void uploadAsSingleFile(final String bucketName,
                                      final String key,
                                      final InputStream inputStream,
                                      final int size) {
        final PutObjectRequest.Builder builder = PutObjectRequest.builder();
        if (storageTierDefined()) {
            builder.storageTier(storageTier);
        }
        final PutObjectRequest putObjectRequest = builder
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(key)
                .putObjectBody(inputStream)
                .build();

        client.putObject(putObjectRequest);
    }

    @Override
    protected CommitMultipartUploadPartDetails _uploadPart(final String bucketName,
                                                           final String key,
                                                           final String uploadId,
                                                           final int partNumber,
                                                           final InputStream in,
                                                           final int actualPartSize) {
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(key)
                .uploadId(uploadId)
                .uploadPartNum(partNumber)
                .uploadPartBody(in)
                .build();

        final UploadPartResponse uploadPartResponse = client.uploadPart(uploadPartRequest);

        return CommitMultipartUploadPartDetails.builder()
                        .partNum(partNumber)
                        .etag(uploadPartResponse.getETag())
                        .build();
    }

    @Override
    protected void abortUpload(final String bucketName, final String key, final String uploadId) {
        final var request = AbortMultipartUploadRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(key)
                .uploadId(uploadId)
                .build();
        client.abortMultipartUpload(request);
    }

    @Override
    protected void completeUpload(final List<CommitMultipartUploadPartDetails> completedParts,
                                  final String bucketName,
                                  final String key,
                                  final String uploadId) {
        final CommitMultipartUploadDetails commitDetails = CommitMultipartUploadDetails.builder()
                .partsToCommit(completedParts)
                .build();

        final CommitMultipartUploadRequest commitMultipartUploadRequest = CommitMultipartUploadRequest.builder()
                .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(key)
                .uploadId(uploadId)
                .commitMultipartUploadDetails(commitDetails)
                .build();

        client.commitMultipartUpload(commitMultipartUploadRequest);
    }

}
