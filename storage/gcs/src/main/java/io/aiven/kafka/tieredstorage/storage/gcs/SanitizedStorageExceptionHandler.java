/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

class SanitizedStorageExceptionHandler {
    static class SanitizedStorageException extends Exception {

        SanitizedStorageException(final String sanitizedMessage) {
            super(sanitizedMessage);
        }
    }

    static SanitizedStorageException sanitizeException(final Exception e) {
        final var fullMessage = new StringBuilder();
        Throwable currentException = e;

        while (currentException != null) {
            if (fullMessage.length() > 0) {
                fullMessage.append("\nCaused by: ");
            }

            fullMessage.append(currentException.getClass().getName()).append(": ")
                    .append(currentException.getMessage());

            currentException = currentException.getCause();
        }
        final var sanitizedMessage = sanitizeMessage(fullMessage.toString());
        return new SanitizedStorageException(sanitizedMessage);
    }

    private static String sanitizeMessage(final String message) {
        final var pattern = Pattern
                .compile("(https?://[^/]+/[^/]+/)(.+?%2F[^%/]+%2F[^%/]+%2F([^%/]+%2F[^%/]+%2F[^%/]+))");
        final var matcher = pattern.matcher(message);

        final var sb = new StringBuffer();
        while (matcher.find()) {
            final var encodedSuffix = matcher.group(3);
            final var decodedSuffix = URLDecoder.decode(encodedSuffix, StandardCharsets.UTF_8);
            final var replacement = "[REDACTED]/" + decodedSuffix;
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);

        return sb.toString();
    }
}
