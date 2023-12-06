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

import java.net.URL;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_AUTH_ENABLED_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_AUTH_KERBEROS_KEYTAB_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_CONF_PREFIX;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_CORE_SITE_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_HDFS_SITE_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_ROOT_CONFIG;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_UPLOAD_BUFFER_SIZE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class HdfsStorageConfigTest {
    @Test
    void minimalConfig() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_ROOT_CONFIG, "/test/subdir"
        ));

        assertThat(config.rootDirectory()).isEqualTo("/test/subdir");
    }

    @Test
    void loadConfigurationFromCoreSite() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_CORE_SITE_CONFIG, getResourcePath("core-site.xml")
        ));
        final Configuration hadoopConf = config.hadoopConf();

        assertThat(hadoopConf.get("fs.defaultFS")).isEqualTo("hdfs://namenode");
        assertThat(hadoopConf.get("my.core.site.config.key")).isEqualTo("my.core.site.config.value");
    }

    @Test
    void loadConfigurationFromHdfsSite() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml")
        ));
        final Configuration hadoopConf = config.hadoopConf();

        assertThat(hadoopConf.get("dfs.replication")).isEqualTo("1");
        assertThat(hadoopConf.get("my.hdfs.site.config.key")).isEqualTo("my.hdfs.site.config.value");
    }

    @Test
    void loadConfigurationFromHdfsConfKeys() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_ROOT_CONFIG, "/test/subdir",
            HDFS_CONF_PREFIX + "fs.defaultFS", "hdfs://test/test",
            HDFS_CONF_PREFIX + "another.key", "another value"
        ));
        final Configuration hadoopConf = config.hadoopConf();

        assertThat(config.rootDirectory()).isEqualTo("/test/subdir");
        assertThat(hadoopConf.get("fs.defaultFS")).isEqualTo("hdfs://test/test");
        assertThat(hadoopConf.get("another.key")).isEqualTo("another value");
        assertThat(hadoopConf.get(HDFS_ROOT_CONFIG)).isNull();
    }

    @Test
    void loadConfigurationFromSeveralSources() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_ROOT_CONFIG, "/test/subdir",
            HDFS_UPLOAD_BUFFER_SIZE_CONFIG, 1024,
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml"),
            HDFS_CORE_SITE_CONFIG, getResourcePath("core-site.xml"),
            HDFS_CONF_PREFIX + "fs.defaultFS", "hdfs://test/test",
            HDFS_CONF_PREFIX + "another.key", "another value"
        ));
        final Configuration hadoopConf = config.hadoopConf();

        assertThat(config.rootDirectory()).isEqualTo("/test/subdir");
        assertThat(config.uploadBufferSize()).isEqualTo(1024);
        assertThat(hadoopConf.get("fs.defaultFS")).isEqualTo("hdfs://test/test");
        assertThat(hadoopConf.get("another.key")).isEqualTo("another value");
        assertThat(hadoopConf.get("my.hdfs.site.config.key")).isEqualTo("my.hdfs.site.config.value");
        assertThat(hadoopConf.get("my.core.site.config.key")).isEqualTo("my.core.site.config.value");
        assertThat(hadoopConf.get("dfs.replication")).isEqualTo("1");
    }

    @Test
    void provideNoOpAuthenticatorByDefault() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml")
        ));

        assertThat(config.hdfsAuthenticator()).isEqualTo(HdfsStorageAuthenticator.noOpAuthenticator());
    }

    @Test
    void provideKerberosAuthenticator() {
        System.setProperty("java.security.krb5.conf", getResourcePath("krb5.conf"));

        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml"),
            HDFS_AUTH_ENABLED_CONFIG, true,
            HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG, "principal@TEST",
            HDFS_AUTH_KERBEROS_KEYTAB_CONFIG, "/path/to/file.keytab",
            HDFS_CONF_PREFIX + "hadoop.security.authentication", "kerberos"
        ));

        assertThat(config.hdfsAuthenticator())
            .isInstanceOf(HdfsStorageConfig.KeytabHdfsStorageAuthenticator.class);

        // TODO checked manually, uncomment when Gitlab CI name resolution problem will be fixed
        // assertThat(config.kerberosPrincipal())
        //    .isEqualTo(String.format("principal/%s@TEST", InetAddress.getLocalHost().getCanonicalHostName()));
    }

    @Test
    void testThrowIfPrincipalNotProvided() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml"),
            HDFS_AUTH_ENABLED_CONFIG, true,
            HDFS_AUTH_KERBEROS_KEYTAB_CONFIG, "/path/to/file.keytab",
            HDFS_CONF_PREFIX + "hadoop.security.authentication", "kerberos"
        ));

        assertThatThrownBy(config::hdfsAuthenticator)
            .isInstanceOf(ConfigException.class)
            .hasMessage("Kerberos principal not provided");
    }

    @Test
    void testThrowIfKeytabNotProvided() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml"),
            HDFS_AUTH_ENABLED_CONFIG, true,
            HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG, "principal@TEST",
            HDFS_CONF_PREFIX + "hadoop.security.authentication", "kerberos"
        ));

        assertThatThrownBy(config::hdfsAuthenticator)
            .isInstanceOf(ConfigException.class)
            .hasMessage("Keytab for kerberos principal not provided");
    }

    @Test
    void testThrowIfWrongAuthMethod() {
        final HdfsStorageConfig config = new HdfsStorageConfig(Map.of(
            HDFS_HDFS_SITE_CONFIG, getResourcePath("hdfs-site.xml"),
            HDFS_AUTH_ENABLED_CONFIG, true,
            HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG, "principal@TEST",
            HDFS_AUTH_KERBEROS_KEYTAB_CONFIG, "/path/to/file.keytab",
            HDFS_CONF_PREFIX + "hadoop.security.authentication", "simple"
        ));

        assertThatThrownBy(config::hdfsAuthenticator)
            .isInstanceOf(ConfigException.class)
            .hasMessage("Currently, only Kerberos is supported as HDFS authentication method");
    }

    private String getResourcePath(final String relativePath) {
        final URL resource = this.getClass().getClassLoader().getResource(relativePath);
        if (resource == null) {
            fail("Resource not found: " + relativePath);
        }
        return resource.getPath();
    }
}
