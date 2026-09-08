/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.JdbcSparkUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import io.openlineage.spark3.agent.utils.ExtensionDataSourceV2Utils;
import io.openlineage.sql.SqlMeta;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.plans.logical.View;
import org.apache.spark.sql.execution.ExternalRDD;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;

/** Traverses LogicalPlan and collect input fields with the corresponding ExprId. */
@Slf4j
public class InputFieldsCollector {

  public static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    discoverInputsFromNode(context, plan);
    CustomCollectorsUtils.collectInputs(context, plan);

    // hacky way to replace `plan instanceof UnaryNode` which fails for Spark 3.2.1
    // because of java.lang.IncompatibleClassChangeError: UnaryNode, but class was expected
    // probably related to single code base for different Spark versions
    if ((plan.getClass()).isAssignableFrom(UnaryNode.class)) {
      collect(context, ((UnaryNode) plan).child());
    } else if (plan instanceof CreateTableAsSelect
        && (plan.children() == null || plan.children().isEmpty())) {
      collect(context, ((CreateTableAsSelect) plan).query());
    } else if (plan.children() != null) {
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(child -> collect(context, child));
    }

    // Subquery expressions (IN (SELECT ...), EXISTS, scalar) hold their inner LogicalPlan
    // in node.expressions, not children(); the analyzed plan still has them embedded
    // because the optimizer's RewritePredicateSubquery has not run yet.
    if (plan.subqueries() != null) {
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.subqueries()).stream()
          .forEach(subPlan -> collect(context, subPlan));
    }
  }

  private static void discoverInputsFromNode(ColumnLevelLineageContext context, LogicalPlan node) {
    List<DatasetIdentifier> datasetIdentifiers = extractDatasetIdentifier(context, node);
    if (isQueryRelationNode(node)) {
      QueryRelationColumnLineageCollector.extractExternalInputs(context, node);
    } else if (hasJdbcSqlColumnLineage(node)) {
      // Skip: JdbcColumnLineageVisitor handles input collection via SqlCollector, which
      // correctly resolves alias names to original column names. Using extractInternalInputs
      // here would add alias names from Spark's output attributes as phantom input fields.
    } else {
      extractInternalInputs(node, context.getBuilder(), datasetIdentifiers);
    }
  }

  private static boolean isQueryRelationNode(LogicalPlan node) {
    if (node instanceof DataSourceV2Relation) {
      return ExtensionDataSourceV2Utils.hasQueryExtensionLineage((DataSourceV2Relation) node);
    }
    if (node instanceof DataSourceV2ScanRelation) {
      return ExtensionDataSourceV2Utils.hasQueryExtensionLineage(
          ((DataSourceV2ScanRelation) node).relation());
    }
    return false;
  }

  /**
   * Returns true if the node is a JDBC relation whose SQL query has been parsed and column lineage
   * is available. In that case, JdbcColumnLineageVisitor/SqlCollector will handle input collection
   * with correct original column names rather than alias names.
   */
  private static boolean hasJdbcSqlColumnLineage(LogicalPlan node) {
    if (!(node instanceof LogicalRelation)) return false;
    if (!(((LogicalRelation) node).relation() instanceof JDBCRelation)) return false;
    JDBCRelation relation = (JDBCRelation) ((LogicalRelation) node).relation();
    return JdbcSparkUtils.extractQueryFromSpark(relation)
        .map(meta -> !meta.columnLineage().isEmpty())
        .orElse(false);
  }

  private static void extractInternalInputs(
      LogicalPlan node,
      ColumnLevelLineageBuilder builder,
      List<DatasetIdentifier> datasetIdentifiers) {

    datasetIdentifiers.stream()
        .forEach(
            di -> {
              ScalaConversionUtils.fromSeq(node.output()).stream()
                  .filter(attr -> attr instanceof AttributeReference)
                  .map(attr -> (AttributeReference) attr)
                  .collect(Collectors.toList())
                  .forEach(
                      attr ->
                          builder.addInput(
                              attr.exprId(),
                              di,
                              attr.name(),
                              attr.dataType().typeName() // Use same approach as schema facet
                              ));
            });
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      ColumnLevelLineageContext context, LogicalPlan node) {
    if (node instanceof DataSourceV2Relation) {
      return extractDatasetIdentifier(context, (DataSourceV2Relation) node);
    } else if (node instanceof DataSourceV2ScanRelation) {
      return extractDatasetIdentifier(context, ((DataSourceV2ScanRelation) node).relation());
    } else if (node instanceof HiveTableRelation) {
      return extractDatasetIdentifier(context, ((HiveTableRelation) node).tableMeta());
    } else if (node instanceof LogicalRelation
        && ((LogicalRelation) node).catalogTable().isDefined()) {
      return extractDatasetIdentifier(context, ((LogicalRelation) node).catalogTable().get());
    } else if (node instanceof LogicalRelation
        && (((LogicalRelation) node).relation() instanceof HadoopFsRelation)) {
      HadoopFsRelation relation = (HadoopFsRelation) ((LogicalRelation) node).relation();
      return extractDatasetIdentifier(relation);
    } else if (node instanceof LogicalRelation
        && ((LogicalRelation) node).relation() instanceof JDBCRelation) {
      JDBCRelation relation = (JDBCRelation) ((LogicalRelation) node).relation();
      return extractDatasetIdentifier(context, relation);
    } else if (node instanceof LogicalRelation
        && context
            .getOlContext()
            .getSparkExtensionVisitorWrapper()
            .isDefinedAt(((LogicalRelation) node).relation())) {
      return extractExtensionDatasetIdentifier(context, (LogicalRelation) node);
    } else if (node instanceof SubqueryAlias) {
      if (isViewSubqueryAlias(node)) {
        SubqueryAlias alias = (SubqueryAlias) node;
        List<String> qualifier = ScalaConversionUtils.fromSeq(alias.identifier().qualifier());
        return extractViewDatasetIdentifier(qualifier, alias.identifier().name());
      }
      // plain user alias or qualified table reference — fall through, children provide the input
    } else if (node instanceof View) {
      return extractDatasetIdentifier(context, (View) node);
    } else if (node instanceof InMemoryRelation) {
      // implemented in
      // io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils.collectInputsAndExpressionDependencies
      // requires merging multiple LogicalPlans
    } else if (node instanceof LogicalRDD) {
      return extractOpaqueSourceDatasetIdentifier((LogicalRDD) node);
    } else if (node instanceof OneRowRelation) {
      return extractOpaqueSourceDatasetIdentifier((OneRowRelation) node);
    } else if (node instanceof LocalRelation) {
      return extractOpaqueSourceDatasetIdentifier((LocalRelation) node);
    } else if (node instanceof ExternalRDD) {
      return extractOpaqueSourceDatasetIdentifier((ExternalRDD<?>) node);
    } else if (node instanceof LeafNode) {
      log.warn("Could not extract dataset identifier from {}", node.getClass().getCanonicalName());
    }

    return Collections.emptyList();
  }

  private static List<DatasetIdentifier> extractOpaqueSourceDatasetIdentifier(OneRowRelation node) {
    return opaqueSourceDatasetIdentifier(node, node.getClass().getSimpleName());
  }

  private static List<DatasetIdentifier> extractOpaqueSourceDatasetIdentifier(LocalRelation node) {
    String streaming = node.isStreaming() ? ",streaming" : "";
    String name = node.getClass().getSimpleName() + "(rows=" + node.data().size() + streaming + ")";
    return opaqueSourceDatasetIdentifier(node, name);
  }

  private static List<DatasetIdentifier> extractOpaqueSourceDatasetIdentifier(ExternalRDD<?> node) {
    DataType dt = node.outputObjAttr().dataType();
    String typeName =
        (dt instanceof ObjectType) ? ((ObjectType) dt).cls().getSimpleName() : dt.typeName();
    String streaming = node.isStreaming() ? ",streaming" : "";
    String name = node.getClass().getSimpleName() + "(type=" + typeName + streaming + ")";
    return opaqueSourceDatasetIdentifier(node, name);
  }

  private static List<DatasetIdentifier> extractOpaqueSourceDatasetIdentifier(LogicalRDD node) {
    List<RDD<?>> fileLikeRdds = Rdds.findFileLikeRdds(node.rdd());
    List<DatasetIdentifier> identifiers = PlanUtils.findDatasetIdentifiers(fileLikeRdds);
    if (!identifiers.isEmpty()) {
      return identifiers.stream()
          .map(di -> new DatasetIdentifier(di.getName(), "opaque-source:" + di.getNamespace()))
          .collect(Collectors.toList());
    }
    String name = node.getClass().getSimpleName() + (node.isStreaming() ? "(streaming)" : "");
    return opaqueSourceDatasetIdentifier(node, name);
  }

  private static List<DatasetIdentifier> opaqueSourceDatasetIdentifier(
      LogicalPlan node, String name) {
    return Collections.singletonList(
        new DatasetIdentifier(name, "opaque-source:" + node.getClass().getName()));
  }

  static List<DatasetIdentifier> extractDatasetIdentifier(
      ColumnLevelLineageContext context, JDBCRelation relation) {
    Optional<SqlMeta> sqlMeta = JdbcSparkUtils.extractQueryFromSpark(relation);
    String jdbcUrl = relation.jdbcOptions().url();
    Properties jdbcProperties = relation.jdbcOptions().asConnectionProperties();
    return sqlMeta
        .map(
            meta ->
                meta.inTables().stream()
                    .map(
                        table ->
                            context
                                .getNamespaceResolver()
                                .resolve(
                                    JdbcDatasetUtils.getDatasetIdentifier(
                                        jdbcUrl, table.qualifiedName(), jdbcProperties)))
                    .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      ColumnLevelLineageContext context, DataSourceV2Relation relation) {
    return DataSourceV2RelationDatasetExtractor.getDatasetIdentifierExtended(
        context.getOlContext(), relation);
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      ColumnLevelLineageContext context, CatalogTable catalogTable) {
    URI location = catalogTable.location();
    if (location == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(
          context
              .getOlContext()
              .getSparkSession()
              .map(s -> PathUtils.fromCatalogTable(catalogTable, s))
              .orElse(
                  new DatasetIdentifier(
                      catalogTable.location().getPath(),
                      PlanUtils.namespaceUri(catalogTable.location()))));
    }
  }

  /* Similar to the InsertIntoHadoopFsRelationVisitor and LogicalRelationDatasetBuilder
   * We need to handle a HadoopFsRelation by extracting that paths it traverses
   * to identify the datasets being used.
   */
  private static List<DatasetIdentifier> extractDatasetIdentifier(HadoopFsRelation relation) {
    List<DatasetIdentifier> inputDatasets = new ArrayList<DatasetIdentifier>();
    List<Path> paths =
        ScalaConversionUtils.fromSeq(relation.location().rootPaths()).stream()
            .collect(Collectors.toList());

    for (Path p : paths) {
      inputDatasets.add(PathUtils.fromURI(p.toUri()));
    }

    return inputDatasets;
  }

  /**
   * Returns true when a SubqueryAlias was produced by Hive virtual view inlining rather than a
   * plain user alias or a qualified table reference. Hive virtual views always have a non-empty
   * qualifier (catalog + schema path) and a body that is NOT a direct table-leaf node. Uses a
   * negative exclusion of known leaf types rather than a positive structural assertion (e.g. "child
   * instanceof Project") so that UNION ALL, DISTINCT, and TABLE views are also matched.
   */
  static boolean isViewSubqueryAlias(LogicalPlan plan) {
    if (!(plan instanceof SubqueryAlias)) return false;
    SubqueryAlias alias = (SubqueryAlias) plan;
    if (ScalaConversionUtils.fromSeq(alias.identifier().qualifier()).isEmpty()) return false;
    LogicalPlan child = alias.child();
    return !(child instanceof HiveTableRelation)
        && !(child instanceof LogicalRelation)
        && !(child instanceof DataSourceV2Relation)
        && !(child instanceof DataSourceV2ScanRelation)
        && !(child instanceof InMemoryRelation);
  }

  private static List<DatasetIdentifier> extractViewDatasetIdentifier(
      List<String> qualifier, String name) {
    List<String> allParts = new ArrayList<>(qualifier);
    allParts.add(name);
    return Collections.singletonList(new DatasetIdentifier(String.join(".", allParts), "View"));
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      ColumnLevelLineageContext context, View view) {
    try {
      Object v2ViewDesc = view.getClass().getMethod("desc").invoke(view);
      Object identifier = v2ViewDesc.getClass().getMethod("identifier").invoke(v2ViewDesc);
      String viewType = view.isTempView() ? "TempView" : "View";

      Optional<String> catalog = Optional.empty();
      Optional<String> database = Optional.empty();
      String table;

      if (identifier instanceof String) {
        Optional<List<String>> parsedOpt =
            context
                .getOlContext()
                .getSparkSession()
                .map(
                    s -> {
                      try {
                        return ScalaConversionUtils.<String>fromSeq(
                            s.sessionState()
                                .sqlParser()
                                .parseMultipartIdentifier((String) identifier));
                      } catch (Exception e) {
                        log.debug(
                            "Could not parse view identifier '{}' via sqlParser; falling back to dot-split",
                            identifier,
                            e);
                        return Arrays.asList(((String) identifier).split("\\."));
                      }
                    });
        List<String> parts =
            parsedOpt.orElseGet(() -> Arrays.asList(((String) identifier).split("\\.")));
        if (parts.size() >= 3) {
          return Collections.singletonList(
              new DatasetIdentifier(String.join(".", parts), viewType));
        } else if (parts.size() == 2) {
          database = Optional.of(parts.get(0));
          table = parts.get(1);
        } else if (!parts.isEmpty()) {
          table = parts.get(0);
        } else {
          return Collections.emptyList();
        }
      } else if (identifier instanceof TableIdentifier) {
        TableIdentifier ti = (TableIdentifier) identifier;
        try {
          Object catalogOpt = ti.getClass().getMethod("catalog").invoke(ti);
          if ((Boolean) catalogOpt.getClass().getMethod("isDefined").invoke(catalogOpt)) {
            catalog =
                Optional.of((String) catalogOpt.getClass().getMethod("get").invoke(catalogOpt));
          }
        } catch (Exception ignored) {
        }
        if (ti.database().isDefined()) database = Optional.of(ti.database().get());
        table = ti.table();
      } else {
        return Collections.emptyList();
      }

      return Collections.singletonList(
          new DatasetIdentifier(
              String.join(".", resolveViewParts(context, catalog, database, table)), viewType));
    } catch (Exception e) {
      log.debug("Could not extract dataset identifier from View", e);
      return Collections.emptyList();
    }
  }

  private static List<String> resolveViewParts(
      ColumnLevelLineageContext context,
      Optional<String> catalog,
      Optional<String> database,
      String table) {
    List<String> parts = new ArrayList<>();
    if (catalog.isPresent()) {
      parts.add(catalog.get());
    } else {
      context
          .getOlContext()
          .getSparkSession()
          .map(
              s ->
                  SQLConf.withExistingConf(
                      s.sessionState().conf(),
                      () -> s.sessionState().catalogManager().currentCatalog().name()))
          .ifPresent(parts::add);
    }
    if (database.isPresent()) {
      parts.add(database.get());
    } else {
      context
          .getOlContext()
          .getSparkSession()
          .ifPresent(
              s ->
                  Collections.addAll(
                      parts,
                      SQLConf.withExistingConf(
                          s.sessionState().conf(),
                          () -> s.sessionState().catalogManager().currentNamespace())));
    }
    parts.add(table);
    return parts;
  }

  private static List<DatasetIdentifier> extractExtensionDatasetIdentifier(
      ColumnLevelLineageContext context, LogicalRelation node) {
    return Collections.singletonList(
        context
            .getOlContext()
            .getSparkExtensionVisitorWrapper()
            .getLineageDatasetIdentifier(node.relation(), context.getEvent().getClass().getName()));
  }
}
