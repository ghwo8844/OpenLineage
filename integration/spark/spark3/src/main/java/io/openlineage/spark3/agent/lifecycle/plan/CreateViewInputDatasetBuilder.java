/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.internal.SQLConf;
import scala.Tuple2;

/**
 * Emits table-level input datasets for a CREATE VIEW statement by re-parsing the view body SQL
 * stored on {@code CreateV2View}. CreateV2View is a LeafCommand — the defining query plan is
 * discarded by the CreateViewAnalysis rule and only the SQL string survives — so without this
 * builder no read→view edges are emitted. We parse the SQL (parser only, no analyzer), walk the
 * tree — including {@link UnresolvedWith} CTE bodies and {@link SubqueryExpression} inner plans,
 * which are not exposed via {@code children()} — to collect every {@link UnresolvedRelation},
 * subtract any single-part references that match a declared CTE alias, and resolve the rest via the
 * existing catalog handlers.
 */
@Slf4j
public class CreateViewInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  private static final String CREATE_V2_VIEW_CLASS =
      "org.apache.spark.sql.catalyst.plans.logical.CreateV2View";

  public CreateViewInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x.getClass().getName().equals(CREATE_V2_VIEW_CLASS);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd;
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    return extractSql(plan).map(this::buildFromSql).orElse(Collections.emptyList());
  }

  List<OpenLineage.InputDataset> buildFromSql(String sql) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    SparkSession session = context.getSparkSession().get();
    LogicalPlan parsed;
    try {
      parsed = session.sessionState().sqlParser().parsePlan(sql);
    } catch (Exception e) {
      log.warn("Could not parse view body SQL; skipping input datasets", e);
      return Collections.emptyList();
    }

    List<List<String>> tableRefs = new ArrayList<>();
    Set<String> cteAliases = new HashSet<>();
    Deque<LogicalPlan> stack = new ArrayDeque<>();
    stack.push(parsed);
    while (!stack.isEmpty()) {
      LogicalPlan node = stack.pop();
      if (node instanceof UnresolvedRelation) {
        tableRefs.add(
            ScalaConversionUtils.fromSeq(((UnresolvedRelation) node).multipartIdentifier()));
      }
      for (LogicalPlan child : ScalaConversionUtils.<LogicalPlan>fromSeq(node.children())) {
        stack.push(child);
      }
      // UnresolvedWith is a UnaryNode whose children() returns only the main query body; the
      // CTE definitions live in a separate cteRelations field that the loop above never sees,
      // so we descend into them explicitly and record the alias names while we're here.
      if (node instanceof UnresolvedWith) {
        for (Tuple2<String, SubqueryAlias> entry :
            ScalaConversionUtils.<Tuple2<String, SubqueryAlias>>fromSeq(
                ((UnresolvedWith) node).cteRelations())) {
          cteAliases.add(entry._1());
          stack.push(entry._2());
        }
      }
      // Subquery expressions (EXISTS, IN (SELECT ...), scalar subqueries) hold their inner
      // LogicalPlan inside the node's expressions, not in children(), so we descend into them
      // explicitly. Each inner plan is its own subtree — correlated references are leaf
      // OuterReference expressions, not back-edges, so this can't introduce cycles.
      for (LogicalPlan subPlan : findSubqueryPlans(node)) {
        stack.push(subPlan);
      }
    }

    Set<DatasetIdentifier> dedup = new LinkedHashSet<>();
    for (List<String> parts : tableRefs) {
      if (parts.isEmpty() || (parts.size() == 1 && cteAliases.contains(parts.get(0)))) {
        continue;
      }
      resolveDatasetIdentifier(session, parts).ifPresent(dedup::add);
    }

    DatasetFactory<OpenLineage.InputDataset> factory = DatasetFactory.input(context);
    return dedup.stream()
        .map(di -> factory.sparkDatasetBuilder().dataset(di).build())
        .collect(Collectors.toList());
  }

  private static List<LogicalPlan> findSubqueryPlans(LogicalPlan node) {
    List<LogicalPlan> plans = new ArrayList<>();
    Deque<Expression> exprStack = new ArrayDeque<>();
    for (Expression expr : ScalaConversionUtils.<Expression>fromSeq(node.expressions())) {
      exprStack.push(expr);
    }
    while (!exprStack.isEmpty()) {
      Expression e = exprStack.pop();
      if (e instanceof SubqueryExpression) {
        plans.add(((SubqueryExpression) e).plan());
      }
      for (Expression child : ScalaConversionUtils.<Expression>fromSeq(e.children())) {
        exprStack.push(child);
      }
    }
    return plans;
  }

  private static Optional<String> extractSql(LogicalPlan plan) {
    try {
      String sql = (String) plan.getClass().getMethod("sql").invoke(plan);
      return sql == null || sql.isEmpty() ? Optional.empty() : Optional.of(sql);
    } catch (ReflectiveOperationException e) {
      log.warn("Could not extract sql() from {}", plan.getClass().getName(), e);
      return Optional.empty();
    }
  }

  /**
   * Splits the relation's parts following Spark's {@code LookupCatalog.CatalogAndIdentifier}: if
   * the first part names a registered catalog, treat it as the catalog; otherwise resolve against
   * the session's current catalog and namespace. Required because downstream handlers (e.g. {@code
   * NetflixIcebergHandler}) read {@code catalogName} separately from the identifier and prepend it
   * to the namespace — a cross-catalog reference would otherwise get double-prefixed.
   */
  private Optional<DatasetIdentifier> resolveDatasetIdentifier(
      SparkSession session, List<String> parts) {
    return SQLConf.withExistingConf(
        session.sessionState().conf(),
        () -> resolveDatasetIdentifier(session.sessionState().catalogManager(), parts));
  }

  private Optional<DatasetIdentifier> resolveDatasetIdentifier(
      CatalogManager catalogManager, List<String> parts) {
    String catalogName;
    String[] namespace;
    String name = parts.get(parts.size() - 1);
    if (parts.size() == 1) {
      catalogName = catalogManager.currentCatalog().name();
      namespace = catalogManager.currentNamespace();
    } else if (catalogManager.isCatalogRegistered(parts.get(0))) {
      catalogName = parts.get(0);
      namespace = parts.subList(1, parts.size() - 1).toArray(new String[0]);
    } else {
      catalogName = catalogManager.currentCatalog().name();
      namespace = parts.subList(0, parts.size() - 1).toArray(new String[0]);
    }

    CatalogPlugin plugin;
    try {
      plugin = catalogManager.catalog(catalogName);
    } catch (Exception e) {
      log.debug("Could not load catalog {} while resolving view input", catalogName, e);
      return Optional.empty();
    }
    if (!(plugin instanceof TableCatalog)) {
      return Optional.empty();
    }

    Identifier ident = Identifier.of(namespace, name);
    try {
      return PlanUtils3.getDatasetIdentifier(
          context, (TableCatalog) plugin, ident, Collections.emptyMap());
    } catch (Exception e) {
      log.debug("Could not resolve dataset identifier for {}.{}", catalogName, ident, e);
      return Optional.empty();
    }
  }
}
