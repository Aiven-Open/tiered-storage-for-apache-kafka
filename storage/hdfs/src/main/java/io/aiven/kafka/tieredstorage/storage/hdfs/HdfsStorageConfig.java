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
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

class HdfsStorageConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    static final String HDFS_CONF_PREFIX = "hdfs.conf.";

    static final String HDFS_ROOT_CONFIG = "hdfs.root";
    private static final String HDFS_ROOT_DEFAULT = "/";
    private static final String HDFS_ROOT_DOC =
        "The base directory path in HDFS relative to which all uploaded file paths will be resolved";

    static final String HDFS_UPLOAD_BUFFER_SIZE_CONFIG = "hdfs.upload.buffer.size";
    private static final int HDFS_UPLOAD_BUFFER_SIZE_DEFAULT = 8192;
    private static final String HDFS_UPLOAD_BUFFER_SIZE_DOC = "Size of the buffer used during file upload";

    static final String HDFS_CORE_SITE_CONFIG = "hdfs.core-site.path";
    private static final String HDFS_CORE_SITE_DOC = "Absolute path of core-site.xml";

    static final String HDFS_HDFS_SITE_CONFIG = "hdfs.hdfs-site.path";
    private static final String HDFS_SITE_DOC = "Absolute path of hdfs-site.xml";

    static final String HDFS_AUTH_ENABLED_CONFIG = "hdfs.auth.enabled";
    private static final boolean HDFS_AUTH_ENABLED_DEFAULT = false;
    private static final String HDFS_AUTH_ENABLED_DOC =
        "Whether to enable Kerberos HDFS authentication. If enabled, corresponding value "
            + "should be provided in 'hadoop.security.authentication' in hadoop XML config files "
            + "or in 'hdfs.conf.hadoop.security.authentication' kafka option";

    static final String HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG = "hdfs.auth.kerberos.principal";
    private static final String HDFS_AUTH_KERBEROS_PRINCIPAL_DOC = "Kerberos principal to be used in HDFS client";

    static final String HDFS_AUTH_KERBEROS_KEYTAB_CONFIG = "hdfs.auth.kerberos.keytab";
    private static final String HDFS_AUTH_KERBEROS_KEYTAB_DOC =
        "Absolute path of the keytab file with the credentials for the principal";

    private final Configuration hadoopConf;

    static {
        CONFIG = new ConfigDef()
            .define(
                HDFS_ROOT_CONFIG,
                ConfigDef.Type.STRING,
                HDFS_ROOT_DEFAULT,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_ROOT_DOC
            )
            .define(
                HDFS_UPLOAD_BUFFER_SIZE_CONFIG,
                ConfigDef.Type.INT,
                HDFS_UPLOAD_BUFFER_SIZE_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                HDFS_UPLOAD_BUFFER_SIZE_DOC
            )
            .define(
                HDFS_CORE_SITE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_CORE_SITE_DOC
            )
            .define(
                HDFS_HDFS_SITE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                HDFS_SITE_DOC
            )
            .define(
                HDFS_AUTH_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                HDFS_AUTH_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                HDFS_AUTH_ENABLED_DOC
            )
            .define(
                HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                HDFS_AUTH_KERBEROS_PRINCIPAL_DOC
            )
            .define(
                HDFS_AUTH_KERBEROS_KEYTAB_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                HDFS_AUTH_KERBEROS_KEYTAB_DOC
            );
    }

    HdfsStorageConfig(final Map<String, ?> props) {
        super(CONFIG, removeHadoopConfProps(props));
        this.hadoopConf = buildHadoopConf(props);
    }

    final String rootDirectory() {
        return getString(HDFS_ROOT_CONFIG);
    }

    final int uploadBufferSize() {
        return getInt(HDFS_UPLOAD_BUFFER_SIZE_CONFIG);
    }

    final boolean isAuthenticationEnabled() {
        return getBoolean(HDFS_AUTH_ENABLED_CONFIG);
    }

    final String kerberosPrincipal() {
        try {
            return SecurityUtil.getServerPrincipal(getString(HDFS_AUTH_KERBEROS_PRINCIPAL_CONFIG), (String) null);
        } catch (final IOException exc) {
            throw new ConfigException("Error resolving kerberos principal name", exc);
        }
    }

    final String kerberosKeytab() {
        return getString(HDFS_AUTH_KERBEROS_KEYTAB_CONFIG);
    }

    final Configuration hadoopConf() {
        return hadoopConf;
    }

    final HdfsStorageAuthenticator hdfsAuthenticator() {
        if (!isAuthenticationEnabled()) {
            return HdfsStorageAuthenticator.noOpAuthenticator();
        }

        if (SecurityUtil.getAuthenticationMethod(hadoopConf)
            != UserGroupInformation.AuthenticationMethod.KERBEROS) {
            throw new ConfigException("Currently, only Kerberos is supported as HDFS authentication method");
        }

        if (kerberosPrincipal() == null) {
            throw new ConfigException("Kerberos principal not provided");
        }

        if (kerberosKeytab() == null) {
            throw new ConfigException("Keytab for kerberos principal not provided");
        }

        return new KeytabHdfsStorageAuthenticator();
    }

    private Configuration buildHadoopConf(final Map<String, ?> props) {
        final Configuration hadoopConf = new Configuration();

        props.entrySet()
            .stream()
            .filter(entry -> entry.getKey().contains(HDFS_CONF_PREFIX))
            .forEach(entry -> hadoopConf.set(
                entry.getKey().replaceFirst(HDFS_CONF_PREFIX, ""),
                entry.getValue().toString()
            ));

        addResourceIfPresent(hadoopConf, HDFS_CORE_SITE_CONFIG);
        addResourceIfPresent(hadoopConf, HDFS_HDFS_SITE_CONFIG);
        return hadoopConf;
    }

    private void addResourceIfPresent(final Configuration hadoopConf, final String configKey) {
        final String resource = getString(configKey);
        if (resource != null) {
            hadoopConf.addResource(new Path("file", null, resource));
        }
    }

    private static Map<String, ?> removeHadoopConfProps(final Map<String, ?> props) {
        return props.entrySet()
            .stream()
            .filter(entry -> !entry.getKey().contains(HDFS_CONF_PREFIX))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));
    }

    class KeytabHdfsStorageAuthenticator implements HdfsStorageAuthenticator {

        public KeytabHdfsStorageAuthenticator() {
            UserGroupInformation.setConfiguration(hadoopConf);
        }

        @Override
        public void authenticate() throws IOException {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal(), kerberosKeytab());
        }
    }
}
