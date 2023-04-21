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

public class DataKeyAndAAD {
    public final SecretKey dataKey;
    public final byte[] aad;

    public DataKeyAndAAD(final SecretKey dataKey, final byte[] aad) {
        this.dataKey = dataKey;
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
        final DataKeyAndAAD that = (DataKeyAndAAD) o;
        return Objects.equals(dataKey, that.dataKey) && Arrays.equals(aad, that.aad);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(dataKey);
        result = 31 * result + Arrays.hashCode(aad);
        return result;
    }
}
