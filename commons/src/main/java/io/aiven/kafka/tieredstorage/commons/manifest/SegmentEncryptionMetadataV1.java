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

package io.aiven.kafka.tieredstorage.commons.manifest;

import javax.crypto.SecretKey;

import java.util.Arrays;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.commons.security.DataKeyAndAAD;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentEncryptionMetadataV1 implements SegmentEncryptionMetadata {
    private final SecretKey dataKey;
    private final byte[] aad;

    @JsonCreator
    public SegmentEncryptionMetadataV1(@JsonProperty(value = "dataKey", required = true)
                                       final SecretKey dataKey,
                                       @JsonProperty(value = "aad", required = true)
                                       final byte[] aad) {
        this.dataKey = Objects.requireNonNull(dataKey, "dataKey cannot be null");
        this.aad = Objects.requireNonNull(aad, "aad cannot be null");
    }

    @Override
    @JsonProperty("dataKey")
    public SecretKey dataKey() {
        return this.dataKey;
    }

    @Override
    @JsonProperty("aad")
    public byte[] aad() {
        return this.aad;
    }

    @Override
    @JsonIgnore
    public DataKeyAndAAD dataKeyAndAAD() {
        return new DataKeyAndAAD(this.dataKey, this.aad);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SegmentEncryptionMetadataV1 that = (SegmentEncryptionMetadataV1) o;

        if (!dataKey.equals(that.dataKey)) {
            return false;
        }
        return Arrays.equals(aad, that.aad);
    }

    @Override
    public int hashCode() {
        int result = dataKey.hashCode();
        result = 31 * result + Arrays.hashCode(aad);
        return result;
    }

    @Override
    public String toString() {
        return "SegmentEncryptionMetadataV1("
            + "dataKey=" + dataKey
            + ", aad=" + Arrays.toString(aad)
            + ")";
    }
}
