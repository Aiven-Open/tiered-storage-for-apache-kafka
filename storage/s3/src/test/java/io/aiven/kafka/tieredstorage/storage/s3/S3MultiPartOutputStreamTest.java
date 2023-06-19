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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());

        final RuntimeException testException = new RuntimeException("test");
        when(mockedS3.uploadPart(any()))
            .thenThrow(testException);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 1, mockedS3);
        assertThatThrownBy(() -> {
            out.write(new byte[] {1});
            out.flush();
        })
            .isInstanceOf(IOException.class)
            .hasMessage("Failed to flush upload part operations")
            .hasRootCause(testException);

        assertThat(out.isClosed()).isTrue();
        // retry close to validate no exception is thrown and number of calls to complete/upload does not change
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingUpload() throws Exception {
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedS3.uploadPart(any()))
            .thenThrow(RuntimeException.class);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 10, mockedS3);

        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .hasMessage("Failed to flush upload part operations")
            .rootCause()
            .isInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingComplete() throws Exception {
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedS3.uploadPart(any()))
            .thenReturn(newUploadPartResult(1, "SOME_ETAG#1"));
        when(mockedS3.completeMultipartUpload(any()))
            .thenThrow(RuntimeException.class);

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 10, mockedS3);

        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .hasMessage("Failed to complete upload transaction")
            .rootCause()
            .isInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void writesOneByte(final boolean withFlush) throws Exception {
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedS3.uploadPart(any()))
            .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
        when(mockedS3.completeMultipartUpload(any()))
            .thenReturn(new CompleteMultipartUploadResult());

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3);
        out.write(1);
        if (withFlush) {
            out.flush();
        }
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3).uploadPart(uploadPartRequestCaptor.capture());
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var value = uploadPartRequestCaptor.getValue();
        assertUploadPartRequest(
            value,
            value.getInputStream().readAllBytes(),
            1,
            1,
            new byte[] {1}
        );
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            Map.of(1, "SOME_ETAG")
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void writesMultipleMessages(final boolean withFlush) throws Exception {
        final int bufferSize = 10;

        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());

        // capturing requests and contents from concurrent threads
        final Map<Integer, UploadPartRequest> uploadPartRequests = new ConcurrentHashMap<>();
        final Map<Integer, byte[]> uploadPartContents = new ConcurrentHashMap<>();
        when(mockedS3.uploadPart(any()))
            .thenAnswer(a -> {
                final UploadPartRequest up = a.getArgument(0);
                //emulate behave of S3 client otherwise we will get wrong array in the memory
                uploadPartRequests.put(up.getPartNumber(), up);
                uploadPartContents.put(up.getPartNumber(), up.getInputStream().readAllBytes());

                return newUploadPartResult(up.getPartNumber(), "SOME_ETAG#" + up.getPartNumber());
            });
        when(mockedS3.completeMultipartUpload(any()))
            .thenReturn(new CompleteMultipartUploadResult());

        final Map<Integer, byte[]> expectedMessageParts = new HashMap<>();
        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedS3);
        for (int i = 0; i < 3; i++) {
            final byte[] message = new byte[bufferSize];
            random.nextBytes(message);
            out.write(message, 0, message.length);
            expectedMessageParts.put(i + 1, message);
        }
        if (withFlush) {
            out.flush();
        }
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3, times(3)).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

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
            Map.of(
                1, "SOME_ETAG#1",
                2, "SOME_ETAG#2",
                3, "SOME_ETAG#3"
            )
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void writesTailMessages(final boolean withFlush) throws Exception {
        final int messageSize = 20;

        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());

        // capturing requests and contents from concurrent threads
        final Map<Integer, UploadPartRequest> uploadPartRequests = new ConcurrentHashMap<>();
        final Map<Integer, byte[]> uploadPartContents = new ConcurrentHashMap<>();
        when(mockedS3.uploadPart(any()))
            .thenAnswer(a -> {
                final UploadPartRequest up = a.getArgument(0);
                //emulate behave of S3 client otherwise we will get wrong array in the memory
                uploadPartRequests.put(up.getPartNumber(), up);
                uploadPartContents.put(up.getPartNumber(), up.getInputStream().readAllBytes());

                return newUploadPartResult(up.getPartNumber(), "SOME_ETAG#" + up.getPartNumber());
            });
        when(mockedS3.completeMultipartUpload(any()))
            .thenReturn(new CompleteMultipartUploadResult());

        // expected messages when no flushing
        final byte[] expectedFullMessage = new byte[messageSize + 10];
        final byte[] expectedTailMessage = new byte[10];
        // expected messages when flushing
        final byte[] firstMessage;
        final byte[] secondMessage;

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedS3);
        {
            final byte[] message = new byte[messageSize];
            random.nextBytes(message);
            out.write(message);
            firstMessage = message;
            System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
        }
        if (withFlush) {
            out.flush();
        }
        {
            final byte[] message = new byte[messageSize];
            random.nextBytes(message);
            out.write(message);
            secondMessage = message;
            System.arraycopy(message, 0, expectedFullMessage, 20, 10);
            System.arraycopy(message, 10, expectedTailMessage, 0, 10);
        }
        if (withFlush) {
            out.flush();
        }
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertThat(uploadPartRequests).hasSize(2);
        if (withFlush) {
            assertUploadPartRequest(uploadPartRequests.get(1), uploadPartContents.get(1), 20, 1, firstMessage);
            assertUploadPartRequest(uploadPartRequests.get(2), uploadPartContents.get(2), 20, 2, secondMessage);
        } else {
            assertUploadPartRequest(uploadPartRequests.get(1), uploadPartContents.get(1), 30, 1, expectedFullMessage);
            assertUploadPartRequest(uploadPartRequests.get(2), uploadPartContents.get(2), 10, 2, expectedTailMessage);
        }

        verify(mockedS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedS3, times(2)).uploadPart(any(UploadPartRequest.class));
        verify(mockedS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            Map.of(
                1, "SOME_ETAG#1",
                2, "SOME_ETAG#2"
            )
        );
    }

    @Test
    void sendAbortIfNoWritingHappened() throws IOException {
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3);
        out.close();

        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();
    }

    @Test
    void failWhenUploadingPartAfterStreamIsClosed() throws IOException {
        when(mockedS3.initiateMultipartUpload(any()))
            .thenReturn(newInitiateMultipartUploadResult());

        final var out = new S3MultiPartOutputStream(BUCKET_NAME, FILE_KEY, 100, mockedS3);
        out.close();

        verify(mockedS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertThatThrownBy(() -> out.write(1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
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
        assertThat(tags).containsExactlyInAnyOrderEntriesOf(expectedETags);
    }

    private static void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest request) {
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getKey()).isEqualTo(FILE_KEY);
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
    }
}
