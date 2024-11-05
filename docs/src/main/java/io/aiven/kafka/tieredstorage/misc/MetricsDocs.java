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

package io.aiven.kafka.tieredstorage.misc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Sanitizer;

import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.fetch.index.MemorySegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.metrics.CaffeineMetricsRegistry;
import io.aiven.kafka.tieredstorage.metrics.MetricsRegistry;
import io.aiven.kafka.tieredstorage.metrics.ThreadPoolMonitorMetricsRegistry;

import static java.lang.System.out;

public class MetricsDocs {
    public static void main(final String[] args) {
        printSectionTitle("Core components metrics");
        out.println(".. Generated from MetricRegistry classes by " + MetricsDocs.class.getCanonicalName());
        out.println();
        printSubsectionTitle("RemoteStorageManager metrics");
        out.println();
        out.println(toRstTable(MetricsRegistry.METRIC_CONTEXT, new MetricsRegistry().all()));

        out.println();
        printSubsectionTitle("SegmentManifestCache metrics");
        out.println();
        out.println(toRstTable(
            CaffeineMetricsRegistry.METRIC_CONTEXT,
            new CaffeineMetricsRegistry(MemorySegmentManifestCache.METRIC_GROUP).all()));
        out.println();
        out.println(toRstTable(
            ThreadPoolMonitorMetricsRegistry.METRIC_CONFIG,
            new ThreadPoolMonitorMetricsRegistry(MemorySegmentManifestCache.THREAD_POOL_METRIC_GROUP).all()));

        out.println();
        printSubsectionTitle("SegmentIndexesCache metrics");
        out.println(toRstTable(
            CaffeineMetricsRegistry.METRIC_CONTEXT,
            new CaffeineMetricsRegistry(MemorySegmentIndexesCache.METRIC_GROUP).all()));
        out.println(toRstTable(
            ThreadPoolMonitorMetricsRegistry.METRIC_CONFIG,
            new ThreadPoolMonitorMetricsRegistry(MemorySegmentIndexesCache.THREAD_POOL_METRIC_GROUP).all()));
        out.println();
        printSubsectionTitle("ChunkCache metrics");
        out.println();
        out.println(toRstTable(
            CaffeineMetricsRegistry.METRIC_CONTEXT,
            new CaffeineMetricsRegistry(ChunkCache.METRIC_GROUP).all()));
        out.println();
        out.println(toRstTable(
            ThreadPoolMonitorMetricsRegistry.METRIC_CONFIG,
            new ThreadPoolMonitorMetricsRegistry(ChunkCache.THREAD_POOL_METRIC_GROUP).all()));

        out.println();
        printSectionTitle("Storage Backend metrics");
        out.println();
        printSubsectionTitle("AzureBlobStorage metrics");
        out.println();
        out.println(toRstTable(
            io.aiven.kafka.tieredstorage.storage.azure.MetricRegistry.METRIC_CONTEXT,
            new io.aiven.kafka.tieredstorage.storage.azure.MetricRegistry().all()));
        out.println();
        printSubsectionTitle("GcsStorage metrics");
        out.println();
        out.println(toRstTable(
            io.aiven.kafka.tieredstorage.storage.gcs.MetricRegistry.METRIC_CONTEXT,
            new io.aiven.kafka.tieredstorage.storage.gcs.MetricRegistry().all()));
        out.println();
        printSubsectionTitle("S3Storage metrics");
        out.println();
        out.println(toRstTable(
            io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry.METRIC_CONTEXT,
            new io.aiven.kafka.tieredstorage.storage.s3.MetricRegistry().all()));
    }

    // o.a.k.common.metrics.Metrics does only have generation of Html documentation.
    // as there is no plans to publish HTML docs, this util method is added to generate RST.
    // may be upstreamed.
    static String toRstTable(final String domain, final Iterable<MetricNameTemplate> allMetrics) {
        final Map<String, Map<String, String>> beansAndAttributes = new TreeMap<>();

        try (final Metrics metrics = new Metrics()) {
            for (final MetricNameTemplate template : allMetrics) {
                final Map<String, String> tags = new LinkedHashMap<>();
                for (final String s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }

                final MetricName metricName = metrics.metricName(
                    template.name(),
                    template.group(),
                    template.description(),
                    tags
                );
                final String beanName = getMBeanName(domain, metricName);
                beansAndAttributes.computeIfAbsent(beanName, k -> new TreeMap<>());
                final Map<String, String> attrAndDesc = beansAndAttributes.get(beanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException(
                        "mBean '" + beanName
                            + "' attribute '"
                            + template.name()
                            + "' is defined twice."
                    );
                }
            }
        }

        final StringBuilder b = new StringBuilder();

        for (final Map.Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            // Add mBean name as a section title
            b.append(e.getKey()).append("\n");
            b.append("=".repeat(e.getKey().length())).append("\n\n");

            // Determine the maximum lengths for each column
            final int maxAttrLength = Math.max("Attribute name".length(),
                    e.getValue().keySet().stream().mapToInt(String::length).max().orElse(0));
            final int maxDescLength = Math.max("Description".length(),
                    e.getValue().values().stream().mapToInt(String::length).max().orElse(0));

            // Create the table header
            final String headerFormat = "%-" + maxAttrLength + "s   %-" + maxDescLength + "s\n";
            final String separatorLine = "=" + "=".repeat(maxAttrLength) + "  " + "=".repeat(maxDescLength) + "\n";

            b.append(separatorLine);
            b.append(String.format(headerFormat, "Attribute name", "Description"));
            b.append(separatorLine);

            // Add table rows
            for (final Map.Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append(String.format(headerFormat, e2.getKey(), e2.getValue()));
            }

            // Close the table
            b.append(separatorLine);
            b.append("\n");  // Add an empty line between tables
        }

        return b.toString();
    }

    // same as o.a.k.common.metrics.JmxReporter#getMBeanName but copy/pasted
    // to avoid adding another dependency to this module.
    static String getMBeanName(final String prefix, final MetricName metricName) {
        final StringBuilder beanName = new StringBuilder();
        beanName.append(prefix);
        beanName.append(":type=");
        beanName.append(metricName.group());
        for (final Map.Entry<String, String> entry : metricName.tags().entrySet()) {
            if (entry.getKey().length() <= 0 || entry.getValue().length() <= 0) {
                continue;
            }
            beanName.append(",");
            beanName.append(entry.getKey());
            beanName.append("=");
            beanName.append(Sanitizer.jmxSanitize(entry.getValue()));
        }
        return beanName.toString();
    }

    static void printSectionTitle(final String title) {
        out.println("=================\n"
            + title + "\n"
            + "=================");
    }

    static void printSubsectionTitle(final String title) {
        out.println("-----------------\n"
            + title + "\n"
            + "-----------------");
    }
}
