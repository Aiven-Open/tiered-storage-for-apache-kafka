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

package io.aiven.kafka.tieredstorage.commons.security;

import javax.crypto.SecretKey;

import java.util.Arrays;
import java.util.Objects;

public class EncryptionKeyAndAAD {
    public final SecretKey key;
    public final byte[] aad;

    public EncryptionKeyAndAAD(final SecretKey key, final byte[] aad) {
        this.key = key;
        this.aad = aad;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EncryptionKeyAndAAD that = (EncryptionKeyAndAAD) o;
        return Objects.equals(key, that.key) && Arrays.equals(aad, that.aad);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(key);
        result = 31 * result + Arrays.hashCode(aad);
        return result;
    }
}
