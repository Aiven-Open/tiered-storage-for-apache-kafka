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

package io.aiven.kafka.tieredstorage.storage.hdfs;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_CONF_PREFIX;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_ROOT_CONFIG;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.http.HttpServer2.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;


public class HdfsStorageMetricsTest {
    private static final String MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";
    private static final String HDFS_ROOT_DIR = "/tmp/test/";
    private static final int UPLOAD_BUFF_SIZE = 4096;
    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private MiniDFSCluster miniDfsCluster;

    private TestMetricCollector metricCollector;

    private HdfsStorage storage;

    private CompletableFuture<?> actionsPerformed;

    @BeforeEach
    void initCluster() throws IOException {
        System.setProperty(MS_INIT_MODE_KEY, "STANDBY");

        final Configuration configuration = new Configuration();
        configuration.set(BIND_ADDRESS, "127.0.0.1");

        miniDfsCluster = new MiniDFSCluster.Builder(configuration)
            .numDataNodes(3)
            .build();
        miniDfsCluster.waitActive();
        miniDfsCluster.getFileSystem().setWorkingDirectory(new Path(HDFS_ROOT_DIR));
    }

    @BeforeEach
    void initStorage() {
        actionsPerformed = new CompletableFuture<>();
        metricCollector = new TestMetricCollector(100L, actionsPerformed);
        storage = new TestHdfsStorage(metricCollector);

        final Map<String, Object> storeConfig = Map.of(
            HDFS_ROOT_CONFIG, "/tmp/test/",
            HDFS_CONF_PREFIX + FS_DEFAULT_NAME_KEY, miniDfsCluster.getURI()
        );
        storage.configure(storeConfig);
    }

    @AfterEach
    void shutdownCluster() {
        miniDfsCluster.shutdown(true);
    }

    @Test
    void metricsShouldBeReported() throws Exception {
        final byte[] data = new byte[UPLOAD_BUFF_SIZE];

        final ObjectKey key = new TestObjectKey("parent_dir/child_dir/x");

        storage.upload(new ByteArrayInputStream(data), key);
        try (final InputStream fetch = storage.fetch(key)) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, BytesRange.of(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);
        storage.delete(Set.of(key));

        actionsPerformed.complete(null);
        metricCollector.statisticsReported
            .get(2L, TimeUnit.SECONDS);

        final ObjectName metricsAttributeName = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage.hdfs:type=hdfs-client-metrics");

        assertThat(MBEAN_SERVER.getAttribute(metricsAttributeName, "file-get-count"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(metricsAttributeName, "file-upload-count"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(metricsAttributeName, "file-delete-count"))
            .isEqualTo(3.0);
        assertThat(MBEAN_SERVER.getAttribute(metricsAttributeName, "file-get-status-count"))
            .isEqualTo(5.0);
        assertThat(MBEAN_SERVER.getAttribute(metricsAttributeName, "directory-create-count"))
            .isEqualTo(2.0);
    }

    private static class TestHdfsStorage extends HdfsStorage {

        private final TestMetricCollector testMetricCollector;

        private TestHdfsStorage(final TestMetricCollector testMetricCollector) {
            this.testMetricCollector = testMetricCollector;
        }

        @Override
        MetricCollector buildMetricCollector(final HdfsStorageConfig config) {
            return testMetricCollector;
        }
    }

    private static class TestMetricCollector extends MetricCollector {

        private final CompletableFuture<?> statisticsReported;
        private final CompletableFuture<?> actionsPerformed;

        TestMetricCollector(final long metricsReportPeriodMs,
                            final CompletableFuture<?> actionsPerformed) {
            super(metricsReportPeriodMs);
            this.actionsPerformed = actionsPerformed;
            this.statisticsReported = new CompletableFuture<>();
        }

        @Override
        protected void reportStatistics() {
            try {
                actionsPerformed.get(5L, TimeUnit.SECONDS);
            } catch (final Exception exception) {
                Assertions.fail(exception);
            }

            super.reportStatistics();
            statisticsReported.complete(null);
        }
    }
}
