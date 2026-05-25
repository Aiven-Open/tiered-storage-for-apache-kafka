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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

/**
 * A wrapper around Iceberg Catalog implementations that provides caching for table operations
 * while delegating namespace operations to the original catalog.
 *
 * @see org.apache.iceberg.CachingCatalog
 */
public class NamespaceAwareCachingCatalog implements Catalog, SupportsNamespaces, Closeable {
    private final Catalog cachingCatalog;
    private final Catalog originalCatalog;

    private NamespaceAwareCachingCatalog(final Catalog originalCatalog, final long cacheExpirationMs) {
        this.cachingCatalog = cacheExpirationMs > 0
            ? CachingCatalog.wrap(originalCatalog, cacheExpirationMs)
            : originalCatalog;
        this.originalCatalog = originalCatalog;
    }

    public static NamespaceAwareCachingCatalog wrap(final Catalog originalCatalog, final long cacheExpirationMs) {
        return new NamespaceAwareCachingCatalog(originalCatalog, cacheExpirationMs);
    }

    public Catalog getOriginalCatalog() {
        return originalCatalog;
    }

    @Override
    public void initialize(final String name, final Map<String, String> properties) {
        originalCatalog.initialize(name, properties);
    }

    @Override
    public String name() {
        return originalCatalog.name();
    }

    @Override
    public Table loadTable(final TableIdentifier identifier) {
        return cachingCatalog.loadTable(identifier);
    }

    @Override
    public boolean tableExists(final TableIdentifier identifier) {
        return cachingCatalog.tableExists(identifier);
    }

    @Override
    public List<TableIdentifier> listTables(final Namespace namespace) {
        return cachingCatalog.listTables(namespace);
    }

    @Override
    public void invalidateTable(final TableIdentifier identifier) {
        cachingCatalog.invalidateTable(identifier);
    }

    @Override
    public TableBuilder buildTable(final TableIdentifier identifier, final Schema schema) {
        return cachingCatalog.buildTable(identifier, schema);
    }

    @Override
    public boolean dropTable(final TableIdentifier identifier) {
        return cachingCatalog.dropTable(identifier);
    }

    @Override
    public boolean dropTable(final TableIdentifier identifier, final boolean purge) {
        return cachingCatalog.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(final TableIdentifier from, final TableIdentifier to) {
        cachingCatalog.renameTable(from, to);
    }

    @Override
    public Table registerTable(final TableIdentifier identifier, final String metadataFileLocation) {
        return cachingCatalog.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public void createNamespace(final Namespace namespace, final Map<String, String> metadata) {
        if (originalCatalog instanceof SupportsNamespaces) {
            ((SupportsNamespaces) originalCatalog).createNamespace(namespace, metadata);
            return;
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public List<Namespace> listNamespaces() {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).listNamespaces();
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public List<Namespace> listNamespaces(final Namespace namespace) throws NoSuchNamespaceException {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).listNamespaces(namespace);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(final Namespace namespace) throws NoSuchNamespaceException {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).loadNamespaceMetadata(namespace);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public boolean dropNamespace(final Namespace namespace) throws NoSuchNamespaceException {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).dropNamespace(namespace);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());

    }

    @Override
    public boolean setProperties(final Namespace namespace, final Map<String, String> properties)
        throws NoSuchNamespaceException {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).setProperties(namespace, properties);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());

    }

    @Override
    public boolean removeProperties(final Namespace namespace, final Set<String> properties)
        throws NoSuchNamespaceException {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).removeProperties(namespace, properties);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public boolean namespaceExists(final Namespace namespace) {
        if (originalCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) originalCatalog).namespaceExists(namespace);
        }
        throw new NoSuchNamespaceException("%s does not support namespaces.",
            originalCatalog.getClass().getCanonicalName());
    }

    @Override
    public void close() throws IOException {
        if (originalCatalog instanceof Closeable) {
            ((Closeable) originalCatalog).close();
        }
    }
}
