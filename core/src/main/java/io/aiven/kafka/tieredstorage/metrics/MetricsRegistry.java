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

package io.aiven.kafka.tieredstorage.metrics;

import java.util.Map;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;

public class MetricsRegistry {

    static final String METRIC_GROUP = "remote-storage-manager-metrics";
    static final String[] TOPIC_TAG_NAMES = {"topic"};
    static final String[] TOPIC_PARTITION_TAG_NAMES = {"topic", "partition"};

    // Segment copy metric names
    static final String SEGMENT_COPY = "segment-copy";
    static final String SEGMENT_COPY_RATE = SEGMENT_COPY + "-rate";
    final MetricNameTemplate segmentCopyRequestsRate = new MetricNameTemplate(SEGMENT_COPY_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyRequestsRateByTopic =
        new MetricNameTemplate(SEGMENT_COPY_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyRequestsRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_TOTAL = SEGMENT_COPY + "-total";
    final MetricNameTemplate segmentCopyRequestsTotal = new MetricNameTemplate(SEGMENT_COPY_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyRequestsTotalByTopic =
        new MetricNameTemplate(SEGMENT_COPY_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyRequestsTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_BYTES = SEGMENT_COPY + "-bytes";
    static final String SEGMENT_COPY_BYTES_RATE = SEGMENT_COPY_BYTES + "-rate";
    final MetricNameTemplate segmentCopyBytesRate = new MetricNameTemplate(SEGMENT_COPY_BYTES_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyBytesRateByTopic =
        new MetricNameTemplate(SEGMENT_COPY_BYTES_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyBytesRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_BYTES_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    public static final String SEGMENT_COPY_BYTES_TOTAL = SEGMENT_COPY_BYTES + "-total";
    final MetricNameTemplate segmentCopyBytesTotal = new MetricNameTemplate(SEGMENT_COPY_BYTES_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyBytesTotalByTopic =
        new MetricNameTemplate(SEGMENT_COPY_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyBytesTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_TIME = SEGMENT_COPY + "-time";
    static final String SEGMENT_COPY_TIME_AVG = SEGMENT_COPY_TIME + "-avg";
    final MetricNameTemplate segmentCopyTimeAvg = new MetricNameTemplate(SEGMENT_COPY_TIME_AVG, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyTimeAvgByTopic =
        new MetricNameTemplate(SEGMENT_COPY_TIME_AVG, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyTimeAvgByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_TIME_AVG, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_TIME_MAX = SEGMENT_COPY_TIME + "-max";
    final MetricNameTemplate segmentCopyTimeMax = new MetricNameTemplate(SEGMENT_COPY_TIME_MAX, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyTimeMaxByTopic =
        new MetricNameTemplate(SEGMENT_COPY_TIME_MAX, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyTimeMaxByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_TIME_MAX, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_ERRORS = SEGMENT_COPY + "-errors";
    static final String SEGMENT_COPY_ERRORS_RATE = SEGMENT_COPY_ERRORS + "-rate";
    final MetricNameTemplate segmentCopyErrorsRate = new MetricNameTemplate(SEGMENT_COPY_ERRORS_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyErrorsRateByTopic =
        new MetricNameTemplate(SEGMENT_COPY_ERRORS_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyErrorsRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_ERRORS_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_COPY_ERRORS_TOTAL = SEGMENT_COPY_ERRORS + "-total";
    final MetricNameTemplate segmentCopyErrorsTotal =
        new MetricNameTemplate(SEGMENT_COPY_ERRORS_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentCopyErrorsTotalByTopic =
        new MetricNameTemplate(SEGMENT_COPY_ERRORS_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentCopyErrorsTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_COPY_ERRORS_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);

    // Segment delete metric names
    static final String SEGMENT_DELETE = "segment-delete";
    static final String SEGMENT_DELETE_RATE = SEGMENT_DELETE + "-rate";
    final MetricNameTemplate segmentDeleteRequestsRate = new MetricNameTemplate(SEGMENT_DELETE_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteRequestsRateByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteRequestsRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_TOTAL = SEGMENT_DELETE + "-total";
    final MetricNameTemplate segmentDeleteRequestsTotal =
        new MetricNameTemplate(SEGMENT_DELETE_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteRequestsTotalByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteRequestsTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_BYTES = SEGMENT_DELETE + "-bytes";
    static final String SEGMENT_DELETE_BYTES_RATE = SEGMENT_DELETE_BYTES + "-rate";
    final MetricNameTemplate segmentDeleteBytesRate =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteBytesRateByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteBytesRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_BYTES_TOTAL = SEGMENT_DELETE_BYTES + "-total";
    final MetricNameTemplate segmentDeleteBytesTotal =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteBytesTotalByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteBytesTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_TIME = SEGMENT_DELETE + "-time";
    static final String SEGMENT_DELETE_TIME_AVG = SEGMENT_DELETE_TIME + "-avg";
    final MetricNameTemplate segmentDeleteTimeAvg = new MetricNameTemplate(SEGMENT_DELETE_TIME_AVG, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteTimeAvgByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_TIME_AVG, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteTimeAvgByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_TIME_AVG, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_TIME_MAX = SEGMENT_DELETE_TIME + "-max";
    final MetricNameTemplate segmentDeleteTimeMax = new MetricNameTemplate(SEGMENT_DELETE_TIME_MAX, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteTimeMaxByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_TIME_MAX, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteTimeMaxByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_TIME_MAX, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_ERRORS = SEGMENT_DELETE + "-errors";
    static final String SEGMENT_DELETE_ERRORS_RATE = SEGMENT_DELETE_ERRORS + "-rate";
    final MetricNameTemplate segmentDeleteErrorsRate =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteErrorsRateByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_RATE, METRIC_GROUP, "",
            TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteErrorsRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_RATE, METRIC_GROUP, "",
            TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_DELETE_ERRORS_TOTAL = SEGMENT_DELETE_ERRORS + "-total";
    final MetricNameTemplate segmentDeleteErrorsTotal =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentDeleteErrorsTotalByTopic =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_TOTAL, METRIC_GROUP, "",
            TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentDeleteErrorsTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_DELETE_ERRORS_TOTAL, METRIC_GROUP, "",
            TOPIC_PARTITION_TAG_NAMES);

    // Segment fetch metric names
    static final String SEGMENT_FETCH = "segment-fetch";
    static final String SEGMENT_FETCH_RATE = SEGMENT_FETCH + "-rate";
    final MetricNameTemplate segmentFetchRequestsRate = new MetricNameTemplate(SEGMENT_FETCH_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentFetchRequestsRateByTopic =
        new MetricNameTemplate(SEGMENT_FETCH_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentFetchRequestsRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_FETCH_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_FETCH_TOTAL = SEGMENT_FETCH + "-total";
    final MetricNameTemplate segmentFetchRequestsTotal = new MetricNameTemplate(SEGMENT_FETCH_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentFetchRequestsTotalByTopic =
        new MetricNameTemplate(SEGMENT_FETCH_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentFetchRequestsTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_FETCH_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_FETCH_REQUESTED_BYTES = SEGMENT_FETCH + "-requested-bytes";
    static final String SEGMENT_FETCH_REQUESTED_BYTES_RATE = SEGMENT_FETCH_REQUESTED_BYTES + "-rate";
    final MetricNameTemplate segmentFetchRequestedBytesRate =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_RATE, METRIC_GROUP, "");
    final MetricNameTemplate segmentFetchRequestedBytesRateByTopic =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_RATE, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentFetchRequestedBytesRateByTopicPartition =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_RATE, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);
    static final String SEGMENT_FETCH_REQUESTED_BYTES_TOTAL = SEGMENT_FETCH_REQUESTED_BYTES + "-total";
    final MetricNameTemplate segmentFetchRequestedBytesTotal =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_TOTAL, METRIC_GROUP, "");
    final MetricNameTemplate segmentFetchRequestedBytesTotalByTopic =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_TAG_NAMES);
    final MetricNameTemplate segmentFetchRequestedBytesTotalByTopicPartition =
        new MetricNameTemplate(SEGMENT_FETCH_REQUESTED_BYTES_TOTAL, METRIC_GROUP, "", TOPIC_PARTITION_TAG_NAMES);

    public static String sensorName(final String name) {
        return name;
    }

    public static String sensorNameByTopic(final TopicPartition topicPartition, final String name) {
        return "topic." + topicPartition.topic() + "." + name;
    }

    public static String sensorNameByTopicPartition(final TopicPartition topicPartition, final String name) {
        return "topic." + topicPartition.topic() + ".partition." + topicPartition.partition() + "." + name;
    }

    static Map<String, String> topicTags(final TopicPartition topicPartition) {
        return Map.of("topic", topicPartition.topic());
    }

    static Map<String, String> topicPartitionTags(final TopicPartition topicPartition) {
        return Map.of(
            "topic", topicPartition.topic(),
            "partition", String.valueOf(topicPartition.partition())
        );
    }
}
