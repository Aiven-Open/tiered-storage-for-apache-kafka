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

package io.aiven.kafka.tieredstorage;

import java.io.InputStream;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.manifest.SegmentFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InternalRemoteStorageManagerSelectorTest {
    @Mock
    InputStream is;
    @Mock
    InternalRemoteStorageManager kafkaRsm;
    @Mock
    InternalRemoteStorageManager icebergRsm;

    @Test
    void onlyKafkaFound() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(kafkaRsm.fetchIndex(any(), any())).thenReturn(is);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.KAFKA, kafkaRsm, null);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verify(kafkaRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void onlyKafkaNotFound() throws RemoteStorageException, SegmentManifestNotFoundException {
        final SegmentManifestNotFoundException exception = new SegmentManifestNotFoundException();
        when(kafkaRsm.fetchIndex(any(), any())).thenThrow(exception);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.KAFKA, kafkaRsm, null);
        assertThatThrownBy(() -> selector.call(irsm -> irsm.fetchIndex(null, null)))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCause(exception);
        verify(kafkaRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void onlyIcebergFound() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(icebergRsm.fetchIndex(any(), any())).thenReturn(is);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.ICEBERG, null, icebergRsm);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verify(icebergRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void onlyIcebergNotFound() throws RemoteStorageException, SegmentManifestNotFoundException {
        final SegmentManifestNotFoundException exception = new SegmentManifestNotFoundException();
        when(icebergRsm.fetchIndex(any(), any())).thenThrow(exception);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.ICEBERG, null, icebergRsm);
        assertThatThrownBy(() -> selector.call(irsm -> irsm.fetchIndex(null, null)))
            .isInstanceOf(RemoteResourceNotFoundException.class)
            .hasCause(exception);
        verify(icebergRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void bothKafkaFirstAndItReturns() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(kafkaRsm.fetchIndex(any(), any())).thenReturn(is);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.KAFKA, kafkaRsm, icebergRsm);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verify(kafkaRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
        verifyNoInteractions(icebergRsm);
    }

    @Test
    void bothKafkaFirstButReturnsIceberg() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(kafkaRsm.fetchIndex(any(), any())).thenThrow(new SegmentManifestNotFoundException());
        when(icebergRsm.fetchIndex(any(), any())).thenReturn(is);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.KAFKA, kafkaRsm, icebergRsm);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verify(kafkaRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
        verify(icebergRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void bothIcebergFirstAndItReturns() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(icebergRsm.fetchIndex(any(), any())).thenReturn(is);
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.ICEBERG, kafkaRsm, icebergRsm);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verifyNoInteractions(kafkaRsm);
        verify(icebergRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void bothIcebergFirstButReturnsKafka() throws RemoteStorageException, SegmentManifestNotFoundException {
        when(kafkaRsm.fetchIndex(any(), any())).thenReturn(is);
        when(icebergRsm.fetchIndex(any(), any())).thenThrow(new SegmentManifestNotFoundException());
        final var selector = new InternalRemoteStorageManagerSelector(SegmentFormat.ICEBERG, kafkaRsm, icebergRsm);
        final InputStream result = selector.call(irsm -> irsm.fetchIndex(null, null));
        assertThat(result).isSameAs(is);
        verify(kafkaRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
        verify(icebergRsm)
            .fetchIndex(isNull(RemoteLogSegmentMetadata.class), isNull(RemoteStorageManager.IndexType.class));
    }

    @Test
    void kafkaMustNotBeNull() {
        assertThatThrownBy(
            () -> new InternalRemoteStorageManagerSelector(SegmentFormat.KAFKA, null, icebergRsm)
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("kafkaRsm cannot be null when format is KAFKA");
    }

    @Test
    void icebergMustNotBeNull() {
        assertThatThrownBy(
            () -> new InternalRemoteStorageManagerSelector(SegmentFormat.ICEBERG, kafkaRsm, null)
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("icebergRsm cannot be null when format is ICEBERG");
    }
}
