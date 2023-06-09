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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3MultiPartOutputStreamTest {

    static final String BUCKET_NAME = "some_bucket";

    static final String FILE_KEY = "some_key";

    static final String UPLOAD_ID = "some_upload_id";

    @Mock
    AmazonS3 mockedS3;

    @Captor
    ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;

    final Random random = new Random();

    @Test
    void sendAbortForAnyExceptionWhileWriting() {
        when(mockedS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        doNothing().when(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        final RuntimeException testException = new RuntimeException("test");
        when(mockedS3.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(testException);

        assertThatThrownBy(() -> {
            try (final S3MultiPartOutputStream out =
                     new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3)) {
                out.write(new byte[] {1, 2, 3});
            }
        }).isInstanceOf(IOException.class).hasRootCause(testException);

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.getValue());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClose() throws Exception {
        when(mockedS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        doNothing().when(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        when(mockedS3.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(RuntimeException.class);

        final S3MultiPartOutputStream out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 10, mockedS3);

        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .rootCause()
            .isInstanceOf(RuntimeException.class);

        verify(mockedS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.getValue());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void writesOneByte() throws Exception {
        when(mockedS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedS3.uploadPart(uploadPartRequestCaptor.capture()))
            .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
        when(mockedS3.completeMultipartUpload(completeMultipartUploadRequestCaptor.capture()))
            .thenReturn(new CompleteMultipartUploadResult());

        try (final S3MultiPartOutputStream out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3)) {
            out.write(1);
        }

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        final UploadPartRequest value = uploadPartRequestCaptor.getValue();
        assertUploadPartRequest(
            value,
            value.getInputStream().readAllBytes(),
            1,
            1,
            new byte[] {1});
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            Map.of(1, "SOME_ETAG")
        );
    }

    @Test
    void writesMultipleMessages() throws Exception {
        final int bufferSize = 10;

        when(mockedS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());

        final Map<Integer, UploadPartRequest> uploadPartRequests = new ConcurrentHashMap<>();
        final Map<Integer, byte[]> uploadPartContents = new ConcurrentHashMap<>();
        when(mockedS3.uploadPart(uploadPartRequestCaptor.capture()))
            .thenAnswer(answer -> {
                final UploadPartRequest up = answer.getArgument(0);
                //emulate behave of S3 client otherwise we will get wrong array in the memory
                uploadPartRequests.put(up.getPartNumber(), up);
                uploadPartContents.put(up.getPartNumber(), up.getInputStream().readAllBytes());

                return newUploadPartResult(up.getPartNumber(), "SOME_TAG#" + up.getPartNumber());
            });
        when(mockedS3.completeMultipartUpload(completeMultipartUploadRequestCaptor.capture()))
            .thenReturn(new CompleteMultipartUploadResult());

        final Map<Integer, byte[]> expectedMessageParts = new HashMap<>();
        try (final S3MultiPartOutputStream out =
                 new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedS3)) {
            for (int i = 0; i < 3; i++) {
                final byte[] message = new byte[bufferSize];
                random.nextBytes(message);
                out.write(message, 0, message.length);
                expectedMessageParts.put(i + 1, message);
            }
        }

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3, times(3)).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        for (final Integer part : expectedMessageParts.keySet()) {
            assertUploadPartRequest(
                uploadPartRequests.get(part),
                uploadPartContents.get(part),
                bufferSize,
                part,
                expectedMessageParts.get(part)
            );
        }
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            Map.of(1, "SOME_TAG#1",
                2, "SOME_TAG#2",
                3, "SOME_TAG#3")
        );
    }

    @Test
    void writesTailMessages() throws Exception {
        final int messageSize = 20;

        final Map<Integer, UploadPartRequest> uploadPartRequests = new ConcurrentHashMap<>();
        final Map<Integer, byte[]> uploadPartContents = new ConcurrentHashMap<>();

        when(mockedS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedS3.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(answer -> {
                final UploadPartRequest up = answer.getArgument(0);
                //emulate behave of S3 client otherwise we will get wrong array in the memory
                uploadPartRequests.put(up.getPartNumber(), up);
                uploadPartContents.put(up.getPartNumber(), up.getInputStream().readAllBytes());

                return newUploadPartResult(up.getPartNumber(), "SOME_TAG#" + up.getPartNumber());
            });
        when(mockedS3.completeMultipartUpload(completeMultipartUploadRequestCaptor.capture()))
            .thenReturn(new CompleteMultipartUploadResult());


        final byte[] expectedFullMessage = new byte[messageSize + 10];
        final byte[] expectedTailMessage = new byte[10];

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedS3);
        {
            final byte[] message = new byte[messageSize];
            random.nextBytes(message);
            out.write(message);
            System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
        }
        {
            final byte[] message = new byte[messageSize];
            random.nextBytes(message);
            out.write(message);
            System.arraycopy(message, 0, expectedFullMessage, 20, 10);
            System.arraycopy(message, 10, expectedTailMessage, 0, 10);
        }
        out.close();

        assertThat(uploadPartRequests).hasSize(2);
        assertUploadPartRequest(uploadPartRequests.get(1), uploadPartContents.get(1), 30, 1, expectedFullMessage);
        assertUploadPartRequest(uploadPartRequests.get(2), uploadPartContents.get(2), 10, 2, expectedTailMessage);

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3, times(2)).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());
    }

    private static InitiateMultipartUploadResult newInitiateMultipartUploadResult() {
        final InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
        initiateMultipartUploadResult.setUploadId(UPLOAD_ID);
        return initiateMultipartUploadResult;
    }

    private static UploadPartResult newUploadPartResult(final int partNumber, final String etag) {
        final UploadPartResult uploadPartResult = new UploadPartResult();
        uploadPartResult.setPartNumber(partNumber);
        uploadPartResult.setETag(etag);
        return uploadPartResult;
    }

    private static void assertUploadPartRequest(final UploadPartRequest uploadPartRequest,
                                                final byte[] bytes,
                                                final int expectedPartSize,
                                                final int expectedPartNumber,
                                                final byte[] expectedBytes) {
        assertThat(uploadPartRequest.getPartSize()).isEqualTo(expectedPartSize);
        assertThat(uploadPartRequest.getUploadId()).isEqualTo(UPLOAD_ID);
        assertThat(uploadPartRequest.getPartNumber()).isEqualTo(expectedPartNumber);
        assertThat(uploadPartRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(uploadPartRequest.getKey()).isEqualTo(FILE_KEY);
        assertThat(bytes).isEqualTo(expectedBytes);
    }

    private static void assertCompleteMultipartUploadRequest(final CompleteMultipartUploadRequest request,
                                                             final Map<Integer, String> expectedETags) {
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getKey()).isEqualTo(FILE_KEY);
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
        final Map<Integer, String> tags = request.getPartETags().stream()
            .collect(Collectors.toMap(PartETag::getPartNumber, PartETag::getETag));
        assertThat(tags).hasSameSizeAs(expectedETags)
            .containsAllEntriesOf(expectedETags);
    }

    private static void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest request) {
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getKey()).isEqualTo(FILE_KEY);
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
    }
}
