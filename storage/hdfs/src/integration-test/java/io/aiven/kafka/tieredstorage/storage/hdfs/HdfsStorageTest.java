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

import java.io.IOException;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_CONF_PREFIX;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_ROOT_CONFIG;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.http.HttpServer2.BIND_ADDRESS;

public class HdfsStorageTest extends BaseStorageTest {

    private static final String MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";

    private MiniDFSCluster miniDfsCluster;

    @BeforeEach
    public void initCluster() throws IOException {
        System.setProperty(MS_INIT_MODE_KEY, "STANDBY");

        final Configuration configuration = new Configuration();
        configuration.set(BIND_ADDRESS, "127.0.0.1");

        miniDfsCluster = new MiniDFSCluster.Builder(configuration)
            .numDataNodes(3)
            .build();
        miniDfsCluster.waitActive();
    }

    @AfterEach
    public void shutdownCluster() {
        miniDfsCluster.shutdown(true);
    }

    @Override
    protected StorageBackend storage() {
        final HdfsStorage hdfsStorage = new HdfsStorage();
        final Map<String, Object> storeConfig = Map.of(
            HDFS_ROOT_CONFIG, "/tmp/test/",
            HDFS_CONF_PREFIX + FS_DEFAULT_NAME_KEY, miniDfsCluster.getURI()
        );
        hdfsStorage.configure(storeConfig);
        return hdfsStorage;
    }
}
