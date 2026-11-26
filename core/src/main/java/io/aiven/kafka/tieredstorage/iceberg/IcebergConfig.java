/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.iceberg;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;

/**
 * The configuration of the Iceberg segment format.
 *
 * <p>It is parsed from the same properties as
 * {@link io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig}, which ignores these keys.
 */
public class IcebergConfig extends AbstractConfig {
    private static final String STRUCTURE_PROVIDER_PREFIX = "structure.provider.";

    private static final String STRUCTURE_PROVIDER_CLASS_CONFIG = STRUCTURE_PROVIDER_PREFIX + "class";
    private static final String STRUCTURE_PROVIDER_CLASS_DOC = "The structure provider implementation class";

    private static final String ICEBERG_PREFIX = "iceberg.";

    private static final String ICEBERG_NAMESPACE_CONFIG = ICEBERG_PREFIX + "namespace";
    private static final String ICEBERG_NAMESPACE_DOC = "The Iceberg namespace";

    private static final String ICEBERG_CATALOG_PREFIX = ICEBERG_PREFIX + "catalog.";

    private static final String ICEBERG_CATALOG_CLASS_CONFIG = ICEBERG_CATALOG_PREFIX + "class";
    private static final String ICEBERG_CATALOG_CLASS_DOC = "The Iceberg catalog implementation class";

    private static final String ICEBERG_CATALOG_CACHE_PREFIX = ICEBERG_CATALOG_PREFIX + "cache.";

    private static final String ICEBERG_CATALOG_CACHE_ENABLED_CONFIG = ICEBERG_CATALOG_CACHE_PREFIX + "enabled";
    private static final String ICEBERG_CATALOG_CACHE_ENABLED_DOC = "Whether to enable caching for Iceberg catalog "
        + "table metadata. When disabled, all catalog operations bypass cache. Default is true.";

    private static final String ICEBERG_CATALOG_CACHE_EXPIRATION_MS_CONFIG = ICEBERG_CATALOG_CACHE_PREFIX
        + "expiration.ms";
    private static final String ICEBERG_CATALOG_CACHE_EXPIRATION_MS_DOC = "Cache expiration time in milliseconds for "
        + "Iceberg catalog table metadata. "
        + "Default is 600000 (10 minutes). "
        + "Higher values reduce catalog backend load but increase risk of stale metadata in multi-writer scenarios.";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            STRUCTURE_PROVIDER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM,
            STRUCTURE_PROVIDER_CLASS_DOC
        );

        configDef.define(
            ICEBERG_NAMESPACE_CONFIG,
            ConfigDef.Type.STRING,
            null,  // null is mapped to Namespace.empty() in icebergNamespace()
            ConfigDef.Importance.MEDIUM,
            ICEBERG_NAMESPACE_DOC
        );

        configDef.define(
            ICEBERG_CATALOG_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM,
            ICEBERG_CATALOG_CLASS_DOC
        );

        configDef.define(
            ICEBERG_CATALOG_CACHE_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            ICEBERG_CATALOG_CACHE_ENABLED_DOC
        );

        configDef.define(
            ICEBERG_CATALOG_CACHE_EXPIRATION_MS_CONFIG,
            ConfigDef.Type.LONG,
            600_000L,
            ConfigDef.Range.atLeast(1L),
            ConfigDef.Importance.MEDIUM,
            ICEBERG_CATALOG_CACHE_EXPIRATION_MS_DOC
        );

        return configDef;
    }

    public IcebergConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    public StructureProvider structureProvider() {
        final Class<?> structureProviderClass = getClass(STRUCTURE_PROVIDER_CLASS_CONFIG);
        if (structureProviderClass == null) {
            return null;
        }
        final StructureProvider structureProvider =
            Utils.newInstance(structureProviderClass, StructureProvider.class);
        structureProvider.configure(this.originalsWithPrefix(STRUCTURE_PROVIDER_PREFIX));
        return structureProvider;
    }

    public Namespace icebergNamespace() {
        final String value = getString(ICEBERG_NAMESPACE_CONFIG);
        if (value == null) {
            return Namespace.empty();
        } else {
            return Namespace.of(value);
        }
    }

    public Catalog icebergCatalog() {
        final Class<?> catalogClass = getClass(ICEBERG_CATALOG_CLASS_CONFIG);
        if (catalogClass == null) {
            return null;
        }
        final Catalog catalog = Utils.newInstance(catalogClass, Catalog.class);
        final Map<String, String> configs = new HashMap<>();
        for (final var entry : originalsWithPrefix(ICEBERG_CATALOG_PREFIX, true).entrySet()) {
            if (entry.getValue() instanceof String) {
                configs.put(entry.getKey(), (String) entry.getValue());
            } else {
                final String message = String.format(
                    "Unknown type of a KV pair in Iceberg config: %s %s", entry.getKey(), entry.getValue());
                throw new ConfigException(message);
            }
        }
        catalog.initialize("catalog", configs);

        final boolean cacheEnabled = getBoolean(ICEBERG_CATALOG_CACHE_ENABLED_CONFIG);
        if (cacheEnabled) {
            final long cacheExpirationMs = getLong(ICEBERG_CATALOG_CACHE_EXPIRATION_MS_CONFIG);
            return NamespaceAwareCachingCatalog.wrap(catalog, cacheExpirationMs);
        } else {
            return catalog;
        }
    }
}
