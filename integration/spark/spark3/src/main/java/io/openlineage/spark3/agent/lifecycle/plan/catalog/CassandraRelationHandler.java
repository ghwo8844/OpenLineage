/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class CassandraRelationHandler implements RelationHandler {

  private static final String CASSANDRA_TABLE_CLASS =
      "org.apache.cassandra.spark.sparksql.CassandraTable";
  private static final String DEFAULT_NAMESPACE = "cassandra";
  private static final int DEFAULT_SIDECAR_PORT = 9043;

  @Override
  public boolean hasClasses() {
    try {
      CassandraRelationHandler.class.getClassLoader().loadClass(CASSANDRA_TABLE_CLASS);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // class not on classpath
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(CASSANDRA_TABLE_CLASS);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // class not on classpath
    }
    return false;
  }

  @Override
  public boolean isClass(DataSourceV2Relation relation) {
    return relation.table().getClass().getName().endsWith("CassandraTable");
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {
    Table table = relation.table();
    String name = table.name(); // returns keyspace.table
    String namespace = extractNamespace(table);
    return new DatasetIdentifier(name, namespace);
  }

  @Override
  public String getName() {
    return "cassandra";
  }

  private String extractNamespace(Table table) {
    try {
      // CassandraTable.dataLayer (private field)
      Field dataLayerField = table.getClass().getDeclaredField("dataLayer");
      dataLayerField.setAccessible(true);
      Object dataLayer = dataLayerField.get(table);

      // DataLayer → ClientConfig via clientConfig() or direct field
      Object clientConfig = invokeMethod(dataLayer, "clientConfig");
      if (clientConfig == null) {
        // Try field access on CassandraDataLayer
        clientConfig = getFieldValue(dataLayer, "clientConfig");
      }

      if (clientConfig != null) {
        String contactPoints = getContactPoints(clientConfig);
        int port = getSidecarPort(clientConfig);

        if (contactPoints != null && !contactPoints.isEmpty()) {
          // Use the first contact point for the namespace
          String firstHost = contactPoints.split(",")[0].trim();
          return String.format("cassandra://%s:%d", firstHost, port);
        }
      }
    } catch (Exception e) {
      log.debug("Could not extract Cassandra connection info via reflection", e);
    }
    return DEFAULT_NAMESPACE;
  }

  private String getContactPoints(Object clientConfig) {
    // Try sidecarContactPoints field
    String value = (String) getFieldValue(clientConfig, "sidecarContactPoints");
    if (value != null) {
      return value;
    }
    // Try getter method
    return (String) invokeMethod(clientConfig, "sidecarContactPoints");
  }

  private int getSidecarPort(Object clientConfig) {
    try {
      Object port = getFieldValue(clientConfig, "sidecarPort");
      if (port instanceof Number) {
        return ((Number) port).intValue();
      }
      Object portResult = invokeMethod(clientConfig, "sidecarPort");
      if (portResult instanceof Number) {
        return ((Number) portResult).intValue();
      }
    } catch (Exception e) {
      // fall through
    }
    return DEFAULT_SIDECAR_PORT;
  }

  private static Object invokeMethod(Object obj, String methodName) {
    try {
      Method method = obj.getClass().getMethod(methodName);
      return method.invoke(obj);
    } catch (Exception e) {
      return null;
    }
  }

  private static Object getFieldValue(Object obj, String fieldName) {
    try {
      Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(obj);
    } catch (Exception e) {
      return null;
    }
  }
}
