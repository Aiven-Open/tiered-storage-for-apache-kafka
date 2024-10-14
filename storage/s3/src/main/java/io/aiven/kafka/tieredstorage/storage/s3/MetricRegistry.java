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

import java.util.List;

import org.apache.kafka.common.MetricNameTemplate;

public class MetricRegistry {
    public static final String METRIC_CONTEXT = "aiven.kafka.server.tieredstorage.s3";
    static final String METRIC_GROUP = "s3-client-metrics";
    static final String GET_OBJECT_REQUESTS = "get-object-requests";
    static final String GET_OBJECT_REQUESTS_RATE = GET_OBJECT_REQUESTS + "-rate";
    static final String GET_OBJECT_REQUESTS_TOTAL = GET_OBJECT_REQUESTS + "-total";
    static final String GET_OBJECT_TIME = "get-object-time";
    static final String GET_OBJECT_TIME_AVG = GET_OBJECT_TIME + "-avg";
    static final String GET_OBJECT_TIME_MAX = GET_OBJECT_TIME + "-max";
    static final String UPLOAD_PART_REQUESTS = "upload-part-requests";
    static final String UPLOAD_PART_REQUESTS_RATE = UPLOAD_PART_REQUESTS + "-rate";
    static final String UPLOAD_PART_REQUESTS_TOTAL = UPLOAD_PART_REQUESTS + "-total";
    static final String UPLOAD_PART_TIME = "upload-part-time";
    static final String UPLOAD_PART_TIME_AVG = UPLOAD_PART_TIME + "-avg";
    static final String UPLOAD_PART_TIME_MAX = UPLOAD_PART_TIME + "-max";
    static final String PUT_OBJECT_REQUESTS = "put-object-requests";
    static final String PUT_OBJECT_REQUESTS_RATE = PUT_OBJECT_REQUESTS + "-rate";
    static final String PUT_OBJECT_REQUESTS_TOTAL = PUT_OBJECT_REQUESTS + "-total";
    static final String PUT_OBJECT_TIME = "put-object-time";
    static final String PUT_OBJECT_TIME_AVG = PUT_OBJECT_TIME + "-avg";
    static final String PUT_OBJECT_TIME_MAX = PUT_OBJECT_TIME + "-max";
    static final String DELETE_OBJECT_REQUESTS = "delete-object-requests";
    static final String DELETE_OBJECT_REQUESTS_RATE = DELETE_OBJECT_REQUESTS + "-rate";
    static final String DELETE_OBJECT_REQUESTS_TOTAL = DELETE_OBJECT_REQUESTS + "-total";
    static final String DELETE_OBJECT_TIME = "delete-object-time";
    static final String DELETE_OBJECT_TIME_AVG = DELETE_OBJECT_TIME + "-avg";
    static final String DELETE_OBJECT_TIME_MAX = DELETE_OBJECT_TIME + "-max";
    static final String DELETE_OBJECTS_REQUESTS = "delete-objects-requests";
    static final String DELETE_OBJECTS_REQUESTS_RATE = DELETE_OBJECTS_REQUESTS + "-rate";
    static final String DELETE_OBJECTS_REQUESTS_TOTAL = DELETE_OBJECTS_REQUESTS + "-total";
    static final String DELETE_OBJECTS_TIME = "delete-objects-time";
    static final String DELETE_OBJECTS_TIME_AVG = DELETE_OBJECTS_TIME + "-avg";
    static final String DELETE_OBJECTS_TIME_MAX = DELETE_OBJECTS_TIME + "-max";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS = "create-multipart-upload-requests";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS_RATE = CREATE_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL = CREATE_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String CREATE_MULTIPART_UPLOAD_TIME = "create-multipart-upload-time";
    static final String CREATE_MULTIPART_UPLOAD_TIME_AVG = CREATE_MULTIPART_UPLOAD_TIME + "-avg";
    static final String CREATE_MULTIPART_UPLOAD_TIME_MAX = CREATE_MULTIPART_UPLOAD_TIME + "-max";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS = "complete-multipart-upload-requests";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE = COMPLETE_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL = COMPLETE_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME = "complete-multipart-upload-time";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME_AVG = COMPLETE_MULTIPART_UPLOAD_TIME + "-avg";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME_MAX = COMPLETE_MULTIPART_UPLOAD_TIME + "-max";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS = "abort-multipart-upload-requests";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS_RATE = ABORT_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL = ABORT_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String ABORT_MULTIPART_UPLOAD_TIME = "abort-multipart-upload-time";
    static final String ABORT_MULTIPART_UPLOAD_TIME_AVG = ABORT_MULTIPART_UPLOAD_TIME + "-avg";
    static final String ABORT_MULTIPART_UPLOAD_TIME_MAX = ABORT_MULTIPART_UPLOAD_TIME + "-max";

    static final MetricNameTemplate GET_OBJECT_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(GET_OBJECT_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(GET_OBJECT_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate GET_OBJECT_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(GET_OBJECT_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate GET_OBJECT_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(GET_OBJECT_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate UPLOAD_PART_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(UPLOAD_PART_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(UPLOAD_PART_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate UPLOAD_PART_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(UPLOAD_PART_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate UPLOAD_PART_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(UPLOAD_PART_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate PUT_OBJECT_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(PUT_OBJECT_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(PUT_OBJECT_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate PUT_OBJECT_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(PUT_OBJECT_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate PUT_OBJECT_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(PUT_OBJECT_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECT_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECT_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECT_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECT_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECT_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECT_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECTS_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECTS_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECTS_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECTS_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate DELETE_OBJECTS_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(DELETE_OBJECTS_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(CREATE_MULTIPART_UPLOAD_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(CREATE_MULTIPART_UPLOAD_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(CREATE_MULTIPART_UPLOAD_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(COMPLETE_MULTIPART_UPLOAD_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(COMPLETE_MULTIPART_UPLOAD_TIME_AVG, METRIC_GROUP, "");
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME =
        new MetricNameTemplate(ABORT_MULTIPART_UPLOAD_REQUESTS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME =
        new MetricNameTemplate(ABORT_MULTIPART_UPLOAD_TIME_MAX, METRIC_GROUP, "");
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME =
        new MetricNameTemplate(ABORT_MULTIPART_UPLOAD_TIME_AVG, METRIC_GROUP, "");

    static final String THROTTLING_ERRORS = "throttling-errors";
    static final String THROTTLING_ERRORS_RATE = THROTTLING_ERRORS + "-rate";
    static final String THROTTLING_ERRORS_TOTAL = THROTTLING_ERRORS + "-total";
    static final String SERVER_ERRORS = "server-errors";
    static final String SERVER_ERRORS_RATE = SERVER_ERRORS + "-rate";
    static final String SERVER_ERRORS_TOTAL = SERVER_ERRORS + "-total";
    static final String CONFIGURED_TIMEOUT_ERRORS = "configured-timeout-errors";
    static final String CONFIGURED_TIMEOUT_ERRORS_RATE = CONFIGURED_TIMEOUT_ERRORS + "-rate";
    static final String CONFIGURED_TIMEOUT_ERRORS_TOTAL = CONFIGURED_TIMEOUT_ERRORS + "-total";
    static final String IO_ERRORS = "io-errors";
    static final String IO_ERRORS_RATE = IO_ERRORS + "-rate";
    static final String IO_ERRORS_TOTAL = IO_ERRORS + "-total";
    static final String OTHER_ERRORS = "other-errors";
    static final String OTHER_ERRORS_RATE = OTHER_ERRORS + "-rate";
    static final String OTHER_ERRORS_TOTAL = OTHER_ERRORS + "-total";

    static final MetricNameTemplate THROTTLING_ERRORS_RATE_METRIC_NAME =
        new MetricNameTemplate(THROTTLING_ERRORS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate THROTTLING_ERRORS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(THROTTLING_ERRORS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate SERVER_ERRORS_RATE_METRIC_NAME =
        new MetricNameTemplate(SERVER_ERRORS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate SERVER_ERRORS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(SERVER_ERRORS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME =
        new MetricNameTemplate(CONFIGURED_TIMEOUT_ERRORS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(CONFIGURED_TIMEOUT_ERRORS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate IO_ERRORS_RATE_METRIC_NAME =
        new MetricNameTemplate(IO_ERRORS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate IO_ERRORS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(IO_ERRORS_TOTAL, METRIC_GROUP, "");
    static final MetricNameTemplate OTHER_ERRORS_RATE_METRIC_NAME =
        new MetricNameTemplate(OTHER_ERRORS_RATE, METRIC_GROUP, "");
    static final MetricNameTemplate OTHER_ERRORS_TOTAL_METRIC_NAME =
        new MetricNameTemplate(OTHER_ERRORS_TOTAL, METRIC_GROUP, "");

    public List<MetricNameTemplate> all() {
        return List.of(
            GET_OBJECT_REQUESTS_RATE_METRIC_NAME,
            GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            GET_OBJECT_TIME_AVG_METRIC_NAME,
            GET_OBJECT_TIME_MAX_METRIC_NAME,
            UPLOAD_PART_REQUESTS_RATE_METRIC_NAME,
            UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME,
            UPLOAD_PART_TIME_AVG_METRIC_NAME,
            UPLOAD_PART_TIME_MAX_METRIC_NAME,
            PUT_OBJECT_REQUESTS_RATE_METRIC_NAME,
            PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            PUT_OBJECT_TIME_AVG_METRIC_NAME,
            PUT_OBJECT_TIME_MAX_METRIC_NAME,
            DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            DELETE_OBJECT_TIME_AVG_METRIC_NAME,
            DELETE_OBJECT_TIME_MAX_METRIC_NAME,
            DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME,
            DELETE_OBJECTS_TIME_AVG_METRIC_NAME,
            DELETE_OBJECTS_TIME_MAX_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            THROTTLING_ERRORS_RATE_METRIC_NAME,
            THROTTLING_ERRORS_TOTAL_METRIC_NAME,
            SERVER_ERRORS_RATE_METRIC_NAME,
            SERVER_ERRORS_TOTAL_METRIC_NAME,
            CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME,
            CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME,
            IO_ERRORS_RATE_METRIC_NAME,
            IO_ERRORS_TOTAL_METRIC_NAME,
            OTHER_ERRORS_RATE_METRIC_NAME,
            OTHER_ERRORS_TOTAL_METRIC_NAME
        );
    }
}
