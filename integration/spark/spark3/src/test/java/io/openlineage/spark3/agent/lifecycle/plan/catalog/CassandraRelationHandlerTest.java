/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.Test;

class CassandraRelationHandlerTest {

  private final CassandraRelationHandler handler = new CassandraRelationHandler();

  @Test
  void testIsClassMatchesCassandraTable() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table().getClass().getName())
        .thenReturn("org.apache.cassandra.spark.sparksql.CassandraTable");
    assertTrue(handler.isClass(relation));
  }

  @Test
  void testIsClassDoesNotMatchOtherTable() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table().getClass().getName())
        .thenReturn("org.apache.spark.sql.connector.catalog.InMemoryTable");
    assertFalse(handler.isClass(relation));
  }

  @Test
  void testGetDatasetIdentifierUsesTableName() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table().name()).thenReturn("system_schema.tables");

    DatasetIdentifier di = handler.getDatasetIdentifier(relation);
    assertEquals("system_schema.tables", di.getName());
    // Falls back to default namespace since mocked table has no DataLayer
    assertEquals("cassandra", di.getNamespace());
  }

  @Test
  void testGetDatasetIdentifierWithKeyspaceTable() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table().name()).thenReturn("my_keyspace.my_table");

    DatasetIdentifier di = handler.getDatasetIdentifier(relation);
    assertEquals("my_keyspace.my_table", di.getName());
    assertEquals("cassandra", di.getNamespace());
  }

  @Test
  void testGetName() {
    assertEquals("cassandra", handler.getName());
  }
}
