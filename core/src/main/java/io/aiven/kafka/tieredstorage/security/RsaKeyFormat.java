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

package io.aiven.kafka.tieredstorage.security;

public enum RsaKeyFormat {
    PEM("pem"),
    PEM_FILE("pem_file");

    public final String name;

    RsaKeyFormat(final String name) {
        this.name = name;
    }

    public static RsaKeyFormat parse(final String value) {
        if (value.equals(PEM.name)) {
            return PEM;
        } else if (value.equals(PEM_FILE.name)) {
            return PEM_FILE;
        } else {
            throw new IllegalArgumentException("Unsupported value: " + value);
        }
    }
}
