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
package io.aiven.kafka.tieredstorage.storage.s3;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * Helper utilities for S3 delete operations.
 *
 * Package-private helper to build Delete XML and compute Content-MD5 for DeleteObjects requests.
 */
final class S3DeleteUtils {
    private S3DeleteUtils() {}

    // Escape XML special characters for the Key element.
    static String escapeXml(String s) {
        if (s == null) return "";
        // Order matters: ampersand first.
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("'", "&apos;")
                .replace("\"", "&quot;");
    }

    /**
     * Build deterministic Delete XML for the given object identifiers and return
     * Base64(MD5(xmlBytes)).
     */
    static String computeDeleteObjectsContentMd5(final List<ObjectIdentifier> objectIdentifiers) {
        Objects.requireNonNull(objectIdentifiers, "objectIdentifiers must not be null");

        StringBuilder sb = new StringBuilder();
        sb.append("<Delete>");
        for (ObjectIdentifier id : objectIdentifiers) {
            sb.append("<Object><Key>")
                    .append(escapeXml(id.key()))
                    .append("</Key></Object>");
        }
        sb.append("</Delete>");

        byte[] xmlBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(xmlBytes);
            return Base64.getEncoder().encodeToString(digest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute MD5 for Delete XML", e);
        }
    }
}
