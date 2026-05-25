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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class NamespaceAwareCachingCatalogTest {

    @Mock
    private Catalog originalCatalog;

    @Mock
    private Table table;

    private TableIdentifier tableIdentifier;

    @BeforeEach
    void setUp() {
        tableIdentifier = TableIdentifier.of("test_namespace", "test_table");
    }

    @Test
    void cachesLoadTableWhenEnabled() {
        when(originalCatalog.loadTable(tableIdentifier)).thenReturn(table);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 60000L);
        
        catalog.loadTable(tableIdentifier);
        catalog.loadTable(tableIdentifier);

        verify(originalCatalog, times(1)).loadTable(tableIdentifier);
    }

    @Test
    void doesNotCacheWhenDisabled() {
        when(originalCatalog.loadTable(tableIdentifier)).thenReturn(table);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        
        catalog.loadTable(tableIdentifier);
        catalog.loadTable(tableIdentifier);

        verify(originalCatalog, times(2)).loadTable(tableIdentifier);
    }

    @Test
    void createNamespaceWithSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);
        
        final Namespace namespace = Namespace.of("test");
        final Map<String, String> metadata = new HashMap<>();

        catalog.createNamespace(namespace, metadata);

        verify((SupportsNamespaces) catalogWithNamespaces).createNamespace(namespace, metadata);
    }

    @Test
    void listNamespacesWithSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final List<Namespace> namespaces = List.of(Namespace.of("ns1"), Namespace.of("ns2"));
        when(((SupportsNamespaces) catalogWithNamespaces).listNamespaces()).thenReturn(namespaces);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.listNamespaces()).isEqualTo(namespaces);
        verify((SupportsNamespaces) catalogWithNamespaces).listNamespaces();
    }

    @Test
    void listNamespacesWithParentIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace parent = Namespace.of("parent");
        final List<Namespace> namespaces = List.of(Namespace.of("parent", "child1"));
        when(((SupportsNamespaces) catalogWithNamespaces).listNamespaces(parent)).thenReturn(namespaces);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.listNamespaces(parent)).isEqualTo(namespaces);
        verify((SupportsNamespaces) catalogWithNamespaces).listNamespaces(parent);
    }

    @Test
    void loadNamespaceMetadataIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace namespace = Namespace.of("test");
        final Map<String, String> metadata = Map.of("key", "value");
        when(((SupportsNamespaces) catalogWithNamespaces).loadNamespaceMetadata(namespace)).thenReturn(metadata);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(metadata);
        verify((SupportsNamespaces) catalogWithNamespaces).loadNamespaceMetadata(namespace);
    }

    @Test
    void dropNamespaceIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace namespace = Namespace.of("test");
        when(((SupportsNamespaces) catalogWithNamespaces).dropNamespace(namespace)).thenReturn(true);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.dropNamespace(namespace)).isTrue();
        verify((SupportsNamespaces) catalogWithNamespaces).dropNamespace(namespace);
    }

    @Test
    void setPropertiesIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace namespace = Namespace.of("test");
        final Map<String, String> properties = Map.of("key", "value");
        when(((SupportsNamespaces) catalogWithNamespaces).setProperties(namespace, properties)).thenReturn(true);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.setProperties(namespace, properties)).isTrue();
        verify((SupportsNamespaces) catalogWithNamespaces).setProperties(namespace, properties);
    }

    @Test
    void removePropertiesIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace namespace = Namespace.of("test");
        final Set<String> properties = Set.of("key1", "key2");
        when(((SupportsNamespaces) catalogWithNamespaces).removeProperties(namespace, properties)).thenReturn(true);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.removeProperties(namespace, properties)).isTrue();
        verify((SupportsNamespaces) catalogWithNamespaces).removeProperties(namespace, properties);
    }

    @Test
    void namespaceExistsIfSupportsNamespaces() {
        final Catalog catalogWithNamespaces = mock(Catalog.class, withSettings()
            .extraInterfaces(SupportsNamespaces.class));
        final Namespace namespace = Namespace.of("test");
        when(((SupportsNamespaces) catalogWithNamespaces).namespaceExists(namespace)).thenReturn(true);

        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(catalogWithNamespaces, 1L);

        assertThat(catalog.namespaceExists(namespace)).isTrue();
        verify((SupportsNamespaces) catalogWithNamespaces).namespaceExists(namespace);
    }

    @Test
    void createNamespaceThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        
        final Namespace namespace = Namespace.of("test");
        final Map<String, String> metadata = new HashMap<>();

        assertThatThrownBy(() -> catalog.createNamespace(namespace, metadata))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void listNamespacesThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);

        assertThatThrownBy(() -> catalog.listNamespaces())
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void listNamespacesWithParentThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace parent = Namespace.of("parent");

        assertThatThrownBy(() -> catalog.listNamespaces(parent))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void loadNamespaceMetadataThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace namespace = Namespace.of("test");

        assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void dropNamespaceThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace namespace = Namespace.of("test");

        assertThatThrownBy(() -> catalog.dropNamespace(namespace))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void setPropertiesThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace namespace = Namespace.of("test");
        final Map<String, String> properties = Map.of("key", "value");

        assertThatThrownBy(() -> catalog.setProperties(namespace, properties))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void removePropertiesThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace namespace = Namespace.of("test");
        final Set<String> properties = Set.of("key1", "key2");

        assertThatThrownBy(() -> catalog.removeProperties(namespace, properties))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void namespaceExistsThrowsExceptionWhenNotSupported() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);
        final Namespace namespace = Namespace.of("test");

        assertThatThrownBy(() -> catalog.namespaceExists(namespace))
            .isInstanceOf(NoSuchNamespaceException.class)
            .hasMessageContaining(originalCatalog.getClass().getCanonicalName())
            .hasMessageContaining("does not support namespaces");
    }

    @Test
    void closeClosesOriginalCatalogIfCloseable() throws IOException {
        final Catalog closeableCatalog = mock(Catalog.class, withSettings()
            .extraInterfaces(Closeable.class));
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(closeableCatalog, 1L);

        catalog.close();

        verify((Closeable) closeableCatalog).close();
    }

    @Test
    void closeDoesNothingIfNotCloseable() {
        final NamespaceAwareCachingCatalog catalog = NamespaceAwareCachingCatalog.wrap(originalCatalog, 1L);

        assertThatCode(catalog::close).doesNotThrowAnyException();

        verify(originalCatalog, never()).name();
    }
}
