/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.sources.BaseRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.immutable.Seq;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class InputFieldsCollectorTest {

  private static final String FILE = "file";
  private static final String SOME_NAME = "some-name";
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  SparkOpenLineageExtensionVisitorWrapper wrapper =
      mock(SparkOpenLineageExtensionVisitorWrapper.class);
  SparkListenerEvent event = mock(SparkListenerEvent.class);
  NamedExpression expression = mock(NamedExpression.class);
  ExprId exprId = mock(ExprId.class);
  DatasetIdentifier di = mock(DatasetIdentifier.class);
  AttributeReference attributeReference = mock(AttributeReference.class);

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(openLineageContext);
    when(context.getEvent()).thenReturn(event);
    when(attributeReference.exprId()).thenReturn(exprId);
    when(attributeReference.name()).thenReturn(SOME_NAME);
    when(openLineageContext.getSparkExtensionVisitorWrapper()).thenReturn(wrapper);
  }

  @Test
  void collectWhenGrandChildNodeIsDataSourceV2Relation() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    LogicalPlan plan = createPlanWithGrandChild(relation);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      when(DataSourceV2RelationDatasetExtractor.getDatasetIdentifierExtended(
              context.getOlContext(), relation))
          .thenReturn(Collections.singletonList(di));
      InputFieldsCollector.collect(context, plan);
    }
    verify(builder, times(1)).addInput(exprId, di, SOME_NAME);
  }

  @Test
  void collectWhenGrandChildNodeIsDataSourceV2ScanRelation() {
    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(scanRelation.relation()).thenReturn(relation);

    LogicalPlan plan = createPlanWithGrandChild(scanRelation);

    when(scanRelation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      when(DataSourceV2RelationDatasetExtractor.getDatasetIdentifierExtended(
              context.getOlContext(), relation))
          .thenReturn(Collections.singletonList(di));
      InputFieldsCollector.collect(context, plan);
    }
    verify(builder, times(1)).addInput(exprId, di, SOME_NAME);
  }

  @Test
  @SneakyThrows
  void collectWhenGrandChildNodeIsHiveTableRelation() {
    HiveTableRelation relation = mock(HiveTableRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    URI uri = new URI("file:/tmp");
    when(relation.tableMeta()).thenReturn(catalogTable);
    when(catalogTable.location()).thenReturn(uri);

    LogicalPlan plan = createPlanWithGrandChild(relation);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    InputFieldsCollector.collect(context, plan);
    verify(builder, times(1)).addInput(exprId, new DatasetIdentifier("/tmp", FILE), SOME_NAME);
  }

  @Test
  @SneakyThrows
  void collectWhenGrandChildNodeIsLogicalRelation() {
    LogicalRelation relation = mock(LogicalRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    URI uri = new URI("file:/tmp");
    when(relation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(catalogTable.location()).thenReturn(uri);

    LogicalPlan plan = createPlanWithGrandChild(relation);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    InputFieldsCollector.collect(context, plan);
    verify(builder, times(1)).addInput(exprId, new DatasetIdentifier("/tmp", FILE), SOME_NAME);
  }

  @Test
  @SneakyThrows
  void collectWhenGrandChildNodeIsLogicalRelationAndCatalogTableNotDefined() {
    LogicalRelation relation = mock(LogicalRelation.class);
    when(relation.catalogTable()).thenReturn(Option.empty());

    LogicalPlan plan = createPlanWithGrandChild(relation);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    InputFieldsCollector.collect(context, plan);
    verify(builder, times(0)).addInput(any(), any(), any());
  }

  @Test
  @SneakyThrows
  void collectWhenGrandChildNodeIsLogicalRelationAndLocationIsEmpty() {
    LogicalRelation relation = mock(LogicalRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(relation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(catalogTable.location()).thenReturn(null);

    LogicalPlan plan = createPlanWithGrandChild(relation);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    InputFieldsCollector.collect(context, plan);
    verify(builder, times(0)).addInput(any(), any(), any());
  }

  @Test
  @SneakyThrows
  void collectWhenGrandChildNodeIsHadoopRelation() {
    LogicalRelation logicalRelation = mock(LogicalRelation.class);
    when(logicalRelation.catalogTable()).thenReturn(Option.empty());
    HadoopFsRelation relation = mock(HadoopFsRelation.class, RETURNS_DEEP_STUBS);
    when(logicalRelation.relation()).thenReturn(relation);

    Path p = new Path("abfss://tmp@storage.dfs.core.windows.net/path");
    Seq<Path> expected_path_seq = ScalaConversionUtils.fromList(Collections.singletonList(p));

    when(relation.location().rootPaths()).thenReturn(expected_path_seq);

    LogicalPlan plan = createPlanWithGrandChild(logicalRelation);

    when(logicalRelation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    InputFieldsCollector.collect(context, plan);
    verify(builder, times(1))
        .addInput(
            exprId,
            new DatasetIdentifier("/path", "abfss://tmp@storage.dfs.core.windows.net"),
            SOME_NAME);
  }

  @Test
  void collectWhenGrandChildNodeIsLineageRelation() {
    LogicalRelation plan = mock(LogicalRelation.class);
    BaseRelation grandChild = mock(BaseRelation.class);
    when(plan.relation()).thenReturn(grandChild);
    when(plan.catalogTable()).thenReturn(Option.empty());

    when(wrapper.isDefinedAt(grandChild)).thenReturn(true);
    when(wrapper.getLineageDatasetIdentifier(grandChild, event.getClass().getName()))
        .thenReturn(di);
    when(plan.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());
    InputFieldsCollector.collect(context, plan);

    verify(builder, times(1)).addInput(exprId, di, SOME_NAME);
  }

  @Test
  void resolveViewPartsUsesQuerySessionSqlConfForFallbacks() {
    SparkSession sparkSession = mock(SparkSession.class);
    SessionState sessionState = mock(SessionState.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    CatalogPlugin currentCatalog = mock(CatalogPlugin.class);
    SQLConf queryConf = new SQLConf();
    SQLConf listenerConf = new SQLConf();

    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.conf()).thenReturn(queryConf);
    when(sessionState.catalogManager()).thenReturn(catalogManager);
    when(currentCatalog.name()).thenReturn("query_catalog");
    when(catalogManager.currentCatalog())
        .thenAnswer(
            invocation -> {
              assertSame(queryConf, SQLConf.get());
              return currentCatalog;
            });
    when(catalogManager.currentNamespace())
        .thenAnswer(
            invocation -> {
              assertSame(queryConf, SQLConf.get());
              return new String[] {"query_namespace"};
            });

    List<String> result =
        SQLConf.withExistingConf(
            listenerConf,
            () -> {
              List<String> parts =
                  resolveViewParts(context, Optional.empty(), Optional.empty(), "view_name");
              assertSame(listenerConf, SQLConf.get());
              return parts;
            });

    assertEquals(Arrays.asList("query_catalog", "query_namespace", "view_name"), result);
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  private static List<String> resolveViewParts(
      ColumnLevelLineageContext context,
      Optional<String> catalog,
      Optional<String> database,
      String table) {
    Method method =
        InputFieldsCollector.class.getDeclaredMethod(
            "resolveViewParts",
            ColumnLevelLineageContext.class,
            Optional.class,
            Optional.class,
            String.class);
    method.setAccessible(true);
    return (List<String>) method.invoke(null, context, catalog, database, table);
  }

  @Test
  @SuppressWarnings("unchecked")
  void collectWhenLogicalRDDWithFileLikeRdds() {
    LogicalRDD logicalRDD = mock(LogicalRDD.class);
    RDD<?> rdd = mock(RDD.class);
    when(logicalRDD.rdd()).thenReturn(rdd);

    LogicalPlan plan = createPlanWithGrandChild(logicalRDD);

    when(logicalRDD.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    DatasetIdentifier resolvedDi = new DatasetIdentifier("/tmp/data", "file");

    try (MockedStatic<Rdds> rddsMock = mockStatic(Rdds.class);
        MockedStatic<PlanUtils> planUtilsMock = mockStatic(PlanUtils.class)) {
      List<RDD<?>> fileLikeRdds = Collections.singletonList(rdd);
      rddsMock.when(() -> Rdds.findFileLikeRdds(rdd)).thenReturn(fileLikeRdds);
      planUtilsMock
          .when(() -> PlanUtils.findDatasetIdentifiers(fileLikeRdds))
          .thenReturn(Collections.singletonList(resolvedDi));

      InputFieldsCollector.collect(context, plan);
    }
    verify(builder, times(1))
        .addInput(exprId, new DatasetIdentifier("/tmp/data", "opaque-source:file"), SOME_NAME);
  }

  @Test
  @SuppressWarnings("unchecked")
  void collectWhenLogicalRDDWithoutFileLikeRdds() {
    LogicalRDD logicalRDD = mock(LogicalRDD.class);
    RDD<?> rdd = mock(RDD.class);
    when(logicalRDD.rdd()).thenReturn(rdd);

    LogicalPlan plan = createPlanWithGrandChild(logicalRDD);

    when(logicalRDD.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic<Rdds> rddsMock = mockStatic(Rdds.class);
        MockedStatic<PlanUtils> planUtilsMock = mockStatic(PlanUtils.class)) {
      rddsMock.when(() -> Rdds.findFileLikeRdds(rdd)).thenReturn(Collections.emptyList());
      planUtilsMock
          .when(() -> PlanUtils.findDatasetIdentifiers(Collections.emptyList()))
          .thenReturn(Collections.emptyList());

      InputFieldsCollector.collect(context, plan);
    }
    verify(builder, times(1))
        .addInput(
            exprId,
            new DatasetIdentifier(
                "LogicalRDD", "opaque-source:org.apache.spark.sql.execution.LogicalRDD"),
            SOME_NAME);
  }

  private LogicalPlan createPlanWithGrandChild(LogicalPlan grandChild) {
    LogicalPlan child =
        new Project(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(expression))
                .asScala()
                .toSeq(),
            grandChild);
    return new CreateTableAsSelect(null, null, null, child, null, null, false);
  }
}
