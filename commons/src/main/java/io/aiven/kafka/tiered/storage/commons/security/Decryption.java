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

package io.aiven.kafka.tiered.storage.commons.security;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;

public interface Decryption {

    default Cipher createDecryptingCipher(final Key key,
                                          final String transformation) {
        return createDecryptingCipher(key, null, transformation);
    }

    default Cipher createDecryptingCipher(final Key key,
                                          final AlgorithmParameterSpec params,
                                          final String transformation) {
        Objects.requireNonNull(key, "key hasn't been set");
        Objects.requireNonNull(transformation, "transformation hasn't been set");
        try {
            final var cipher = Cipher.getInstance(transformation, "BC");
            if (Objects.nonNull(params)) {
                cipher.init(
                        Cipher.DECRYPT_MODE,
                        key,
                        params,
                        SecureRandom.getInstanceStrong());
            } else {
                cipher.init(
                        Cipher.DECRYPT_MODE,
                        key,
                        SecureRandom.getInstanceStrong());
            }
            return cipher;
        } catch (final NoSuchAlgorithmException
                | NoSuchPaddingException
                | InvalidKeyException
                | InvalidAlgorithmParameterException
                | NoSuchProviderException e) {
            throw new RuntimeException("Couldn't create decrypt cipher", e);
        }
    }

}
