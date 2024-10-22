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

import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;

public class MetricRegistry {
    public static final String METRIC_CONTEXT = "aiven.kafka.server.tieredstorage.gcs";

    static final String METRIC_GROUP = "gcs-client-metrics";
    static final String OBJECT_METADATA_GET = "object-metadata-get";
    static final String OBJECT_METADATA_GET_RATE = OBJECT_METADATA_GET + "-rate";
    static final String OBJECT_METADATA_GET_TOTAL = OBJECT_METADATA_GET + "-total";
    static final String OBJECT_METADATA_GET_DOC = "get object metadata operations";
    static final String OBJECT_GET = "object-get";
    static final String OBJECT_GET_RATE = OBJECT_GET + "-rate";
    static final String OBJECT_GET_TOTAL = OBJECT_GET + "-total";
    static final String OBJECT_GET_DOC = "get object operations";
    static final String OBJECT_DELETE = "object-delete";
    static final String OBJECT_DELETE_RATE = OBJECT_DELETE + "-rate";
    static final String OBJECT_DELETE_TOTAL = OBJECT_DELETE + "-total";
    static final String OBJECT_DELETE_DOC = "delete object operations";
    static final String RESUMABLE_UPLOAD_INITIATE = "resumable-upload-initiate";
    static final String RESUMABLE_UPLOAD_INITIATE_RATE = RESUMABLE_UPLOAD_INITIATE + "-rate";
    static final String RESUMABLE_UPLOAD_INITIATE_TOTAL = RESUMABLE_UPLOAD_INITIATE + "-total";
    static final String RESUMABLE_UPLOAD_INITIATE_DOC = "initiate resumable upload operations";
    static final String RESUMABLE_CHUNK_UPLOAD = "resumable-chunk-upload";
    static final String RESUMABLE_CHUNK_UPLOAD_RATE = RESUMABLE_CHUNK_UPLOAD + "-rate";
    static final String RESUMABLE_CHUNK_UPLOAD_TOTAL = RESUMABLE_CHUNK_UPLOAD + "-total";
    static final String RESUMABLE_CHUNK_UPLOAD_DOC = "upload chunk operations as part of resumable upload";

    private static final String RATE_DOC_PREFIX = "Rate of ";
    private static final String TOTAL_DOC_PREFIX = "Total number of ";

    static final MetricNameTemplate OBJECT_METADATA_GET_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_METADATA_GET_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_METADATA_GET_DOC
    );
    static final MetricNameTemplate OBJECT_METADATA_GET_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_METADATA_GET_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_METADATA_GET_DOC
    );
    static final MetricNameTemplate OBJECT_GET_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_GET_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_GET_DOC
    );
    static final MetricNameTemplate OBJECT_GET_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_GET_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_GET_DOC
    );
    static final MetricNameTemplate OBJECT_DELETE_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_DELETE_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_DELETE_DOC
    );
    static final MetricNameTemplate OBJECT_DELETE_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_DELETE_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_DELETE_DOC
    );
    static final MetricNameTemplate RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_UPLOAD_INITIATE_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + RESUMABLE_UPLOAD_INITIATE_DOC
    );
    static final MetricNameTemplate RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_UPLOAD_INITIATE_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + RESUMABLE_UPLOAD_INITIATE_DOC
    );
    static final MetricNameTemplate RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_CHUNK_UPLOAD_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + RESUMABLE_CHUNK_UPLOAD_DOC
    );
    static final MetricNameTemplate RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_CHUNK_UPLOAD_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + RESUMABLE_CHUNK_UPLOAD_DOC
    );

    public List<MetricNameTemplate> all() {
        return List.of(
            OBJECT_METADATA_GET_RATE_METRIC_NAME,
            OBJECT_METADATA_GET_TOTAL_METRIC_NAME,
            OBJECT_GET_RATE_METRIC_NAME,
            OBJECT_GET_TOTAL_METRIC_NAME,
            OBJECT_DELETE_RATE_METRIC_NAME,
            OBJECT_DELETE_TOTAL_METRIC_NAME,
            RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME,
            RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME,
            RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME,
            RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME
        );
    }
}
