/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NetflixIcebergHandlerTest {

  private final NetflixIcebergHandler handler =
      new NetflixIcebergHandler(mock(OpenLineageContext.class));
  private final TableCatalog catalog = mock(TableCatalog.class);
  private final Table table = mock(Table.class);
  private final Identifier identifier = Identifier.of(new String[] {"database"}, "table");

  @Test
  void returnsNumericSnapshotId() throws NoSuchTableException {
    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.properties())
        .thenReturn(Collections.singletonMap("current-snapshot-id", "1500100900"));

    Optional<String> version =
        handler.getDatasetVersion(catalog, identifier, Collections.emptyMap());

    assertThat(version).contains("1500100900");
  }

  @ParameterizedTest
  @ValueSource(strings = {"none", "not-a-number"})
  void skipsNonNumericSnapshotId(String snapshotId) throws NoSuchTableException {
    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.properties())
        .thenReturn(Collections.singletonMap("current-snapshot-id", snapshotId));

    Optional<String> version =
        handler.getDatasetVersion(catalog, identifier, Collections.emptyMap());

    assertThat(version).isEmpty();
  }

  @Test
  void skipsUnexpectedDatasetVersionFailure() throws NoSuchTableException {
    when(catalog.loadTable(identifier)).thenThrow(new RuntimeException("unexpected"));

    Optional<String> version =
        handler.getDatasetVersion(catalog, identifier, Collections.emptyMap());

    assertThat(version).isEmpty();
  }

  @Test
  void buildsCatalogQualifiedMetadataTableName() {
    Identifier metadataIdentifier =
        Identifier.of(new String[] {"mvd", "comp_intel_titles_current"}, "snapshots");

    assertThat(
            NetflixIcebergHandler.getCatalogQualifiedTableName(
                "prodhive", metadataIdentifier))
        .isEqualTo("prodhive.mvd.comp_intel_titles_current.snapshots");
  }

  @Test
  void buildsCatalogQualifiedRegularTableNamedSnapshots() {
    Identifier regularTableIdentifier = Identifier.of(new String[] {"mvd"}, "snapshots");

    assertThat(
            NetflixIcebergHandler.getCatalogQualifiedTableName(
                "prodhive", regularTableIdentifier))
        .isEqualTo("prodhive.mvd.snapshots");
  }

  @Test
  void buildsQualifiedNameWhenDatabaseHasSameNameAsCatalog() {
    Identifier metadataIdentifier =
        Identifier.of(new String[] {"prodhive", "titles"}, "snapshots");

    assertThat(
            NetflixIcebergHandler.getCatalogQualifiedTableName(
                "prodhive", metadataIdentifier))
        .isEqualTo("prodhive.prodhive.titles.snapshots");
  }

  @Test
  void buildsCatalogQualifiedRegularTableName() {
    Identifier regularTableIdentifier =
        Identifier.of(new String[] {"mvd", "titles"}, "current");

    assertThat(
            NetflixIcebergHandler.getCatalogQualifiedTableName(
                "prodhive", regularTableIdentifier))
        .isEqualTo("prodhive.mvd.titles.current");
  }

  @Test
  void buildsCatalogQualifiedNameWithoutNamespace() {
    Identifier identifier = Identifier.of(new String[0], "table");

    assertThat(NetflixIcebergHandler.getCatalogQualifiedTableName("prodhive", identifier))
        .isEqualTo("prodhive.table");
  }
}
