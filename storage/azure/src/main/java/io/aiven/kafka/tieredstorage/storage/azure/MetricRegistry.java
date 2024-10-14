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

package io.aiven.kafka.tieredstorage.storage.azure;

import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;

public class MetricRegistry {
    static final String METRIC_GROUP = "azure-blob-storage-client-metrics";
    static final String BLOB_DELETE = "blob-delete";
    static final String BLOB_DELETE_RATE = BLOB_DELETE + "-rate";
    static final String BLOB_DELETE_TOTAL = BLOB_DELETE + "-total";
    static final String BLOB_UPLOAD = "blob-upload";
    static final String BLOB_UPLOAD_RATE = BLOB_UPLOAD + "-rate";
    static final String BLOB_UPLOAD_TOTAL = BLOB_UPLOAD + "-total";
    static final String BLOCK_UPLOAD = "block-upload";
    static final String BLOCK_UPLOAD_RATE = BLOCK_UPLOAD + "-rate";
    static final String BLOCK_UPLOAD_TOTAL = BLOCK_UPLOAD + "-total";
    static final String BLOCK_LIST_UPLOAD = "block-list-upload";
    static final String BLOCK_LIST_UPLOAD_RATE = BLOCK_LIST_UPLOAD + "-rate";
    static final String BLOCK_LIST_UPLOAD_TOTAL = BLOCK_LIST_UPLOAD + "-total";
    static final String BLOB_GET = "blob-get";
    static final String BLOB_GET_RATE = BLOB_GET + "-rate";
    static final String BLOB_GET_TOTAL = BLOB_GET + "-total";

    static final MetricNameTemplate BLOB_DELETE_RATE_METRIC_NAME =
        new MetricNameTemplate(BLOB_DELETE_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate BLOB_DELETE_TOTAL_METRIC_NAME =
        new MetricNameTemplate(BLOB_DELETE_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate BLOB_UPLOAD_RATE_METRIC_NAME =
        new MetricNameTemplate(BLOB_UPLOAD_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate BLOB_UPLOAD_TOTAL_METRIC_NAME =
        new MetricNameTemplate(BLOB_UPLOAD_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate BLOCK_UPLOAD_RATE_METRIC_NAME =
        new MetricNameTemplate(BLOCK_UPLOAD_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate BLOCK_UPLOAD_TOTAL_METRIC_NAME =
        new MetricNameTemplate(BLOCK_UPLOAD_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate BLOCK_LIST_UPLOAD_RATE_METRIC_NAME =
        new MetricNameTemplate(BLOCK_LIST_UPLOAD_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME =
        new MetricNameTemplate(BLOCK_LIST_UPLOAD_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate BLOB_GET_RATE_METRIC_NAME = new MetricNameTemplate(BLOB_GET_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate BLOB_GET_TOTAL_METRIC_NAME =
        new MetricNameTemplate(BLOB_GET_TOTAL, METRIC_GROUP, "");

    public List<MetricNameTemplate> all() {
        return List.of(
            BLOB_DELETE_RATE_METRIC_NAME,
            BLOB_DELETE_TOTAL_METRIC_NAME,
            BLOB_UPLOAD_RATE_METRIC_NAME,
            BLOB_UPLOAD_TOTAL_METRIC_NAME,
            BLOCK_UPLOAD_RATE_METRIC_NAME,
            BLOCK_UPLOAD_TOTAL_METRIC_NAME,
            BLOCK_LIST_UPLOAD_RATE_METRIC_NAME,
            BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME,
            BLOB_GET_RATE_METRIC_NAME,
            BLOB_GET_TOTAL_METRIC_NAME
        );
    }
}
