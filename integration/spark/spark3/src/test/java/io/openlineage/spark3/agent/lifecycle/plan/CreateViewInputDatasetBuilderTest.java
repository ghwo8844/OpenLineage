/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CreateViewInputDatasetBuilderTest {

  private static SparkSession session;
  private OpenLineageContext context;
  private CreateViewInputDatasetBuilder builder;

  @BeforeAll
  static void startSpark() {
    SparkConf conf =
        new SparkConf().setAppName("CreateViewInputDatasetBuilderTest").setMaster("local[1]");
    session = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (session != null) {
      session.stop();
    }
  }

  private OpenLineageContext newContext() {
    return OpenLineageContext.builder()
        .sparkSession(session)
        .sparkContext(mock(SparkContext.class))
        .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
        .meterRegistry(new SimpleMeterRegistry())
        .openLineageConfig(new SparkOpenLineageConfig())
        .build();
  }

  private CreateViewInputDatasetBuilder newBuilder(OpenLineageContext ctx) {
    return new CreateViewInputDatasetBuilder(ctx);
  }

  @Test
  void qualifiedThreePartIdentifierResolvesToSingleInput() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenReturn(Optional.of(new DatasetIdentifier("sales.orders", "prodhive")));

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql("SELECT a, b FROM prodhive.sales.orders");

      assertThat(inputs).hasSize(1);
      assertThat(inputs.get(0).getName()).isEqualTo("sales.orders");
      assertThat(inputs.get(0).getNamespace()).isEqualTo("prodhive");
    }
  }

  @Test
  void unqualifiedIdentifierIsResolvedNotFiltered() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenReturn(Optional.of(new DatasetIdentifier("default.orders", "prodhive")));

      List<OpenLineage.InputDataset> inputs = builder.buildFromSql("SELECT * FROM orders");

      // Unqualified reads resolve via the session's current catalog + namespace.
      assertThat(inputs).hasSize(1);
      assertThat(inputs.get(0).getName()).isEqualTo("default.orders");
    }
  }

  @Test
  void catalogResolutionUsesQuerySessionSqlConf() {
    context = newContext();
    builder = newBuilder(context);
    SQLConf queryConf = session.sessionState().conf();
    SQLConf listenerConf = new SQLConf();

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenAnswer(
              invocation -> {
                assertThat(SQLConf.get()).isSameAs(queryConf);
                return Optional.of(new DatasetIdentifier("default.orders", "prodhive"));
              });

      List<OpenLineage.InputDataset> inputs =
          SQLConf.withExistingConf(
              listenerConf,
              () -> {
                List<OpenLineage.InputDataset> resolved =
                    builder.buildFromSql("SELECT * FROM orders");
                assertThat(SQLConf.get()).isSameAs(listenerConf);
                return resolved;
              });

      assertThat(inputs).hasSize(1);
    }
  }

  @Test
  void cteAliasIsFilteredButInnerRealTableRemains() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenAnswer(
              inv -> {
                Identifier id = inv.getArgument(2);
                String[] ns = id.namespace();
                String key = (ns.length == 0 ? "" : String.join(".", ns)) + ":" + id.name();
                return Optional.of(new DatasetIdentifier(key, "prodhive"));
              });

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql("WITH c AS (SELECT x FROM prodhive.sales.orders) SELECT * FROM c");

      assertThat(inputs).hasSize(1);
      assertThat(inputs.get(0).getName()).isEqualTo("sales:orders");
    }
  }

  @Test
  void multipleCteBodiesAreWalked() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenAnswer(
              inv -> {
                Identifier id = inv.getArgument(2);
                return Optional.of(new DatasetIdentifier(id.name(), "prodhive"));
              });

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql(
              "WITH "
                  + "  cte_a AS (SELECT id FROM prodhive.bdp.t_a), "
                  + "  cte_b AS (SELECT id FROM prodhive.bdp.t_b), "
                  + "  cte_c AS (SELECT id FROM prodhive.bdp.t_c), "
                  + "  rollup AS (SELECT id FROM cte_a UNION ALL SELECT id FROM cte_b) "
                  + "SELECT * FROM cte_a a JOIN cte_b b ON a.id = b.id "
                  + "JOIN cte_c c ON a.id = c.id JOIN rollup r ON r.id = a.id");

      assertThat(inputs)
          .extracting(OpenLineage.InputDataset::getName)
          .containsExactlyInAnyOrder("t_a", "t_b", "t_c");
    }
  }

  @Test
  void nestedSubqueriesContributeInputs() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenAnswer(
              inv -> {
                Identifier id = inv.getArgument(2);
                return Optional.of(new DatasetIdentifier(id.name(), "prodhive"));
              });

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql(
              "SELECT * FROM prodhive.db.a "
                  + "JOIN prodhive.db.b ON a.id = b.id "
                  + "WHERE EXISTS (SELECT 1 FROM prodhive.db.c)");

      assertThat(inputs)
          .extracting(OpenLineage.InputDataset::getName)
          .containsExactlyInAnyOrder("a", "b", "c");
    }
  }

  @Test
  void malformedSqlYieldsEmptyList() {
    context = newContext();
    builder = newBuilder(context);

    List<OpenLineage.InputDataset> inputs = builder.buildFromSql("this is not sql");

    assertThat(inputs).isEmpty();
  }

  @Test
  void resolutionFailureYieldsEmptyList() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenReturn(Optional.empty());

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql("SELECT * FROM prodhive.sales.orders");

      assertThat(inputs).isEmpty();
    }
  }

  @Test
  void duplicateReferencesAreDeduplicated() {
    context = newContext();
    builder = newBuilder(context);

    try (MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class)) {
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      any(OpenLineageContext.class),
                      any(TableCatalog.class),
                      any(Identifier.class),
                      any()))
          .thenReturn(Optional.of(new DatasetIdentifier("sales.orders", "prodhive")));

      List<OpenLineage.InputDataset> inputs =
          builder.buildFromSql("SELECT * FROM prodhive.sales.orders o1, prodhive.sales.orders o2");

      assertThat(inputs).hasSize(1);
    }
  }
}
