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

package io.aiven.kafka.tieredstorage.config;

import java.nio.file.Path;
import java.util.Objects;

public class KeyPairPaths {
    public final Path publicKey;
    public final Path privateKey;

    KeyPairPaths(final Path publicKey, final Path privateKey) {
        this.publicKey = Objects.requireNonNull(publicKey, "publicKey cannot be null");
        this.privateKey = Objects.requireNonNull(privateKey, "privateKey cannot be null");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyPairPaths that = (KeyPairPaths) o;
        return Objects.equals(publicKey, that.publicKey) && Objects.equals(privateKey, that.privateKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicKey, privateKey);
    }

    @Override
    public String toString() {
        return "KeyPairPaths("
            + "publicKey=" + publicKey
            + ", privateKey=" + privateKey
            + ")";
    }
}
