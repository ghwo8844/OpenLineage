/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.View;

/**
 * Collects analyzed-plan expression dependencies only along branches that lead to views.
 *
 * <p>The optimized plan provides dependencies for physical datasets, but optimization can remove a
 * logical {@link View} and replace its output expression IDs with IDs from the underlying table.
 * This collector preserves the complementary path from query outputs and predicates to the view's
 * analyzed expression IDs. Within an expanded view definition, it continues looking for nested
 * views but collects only operators on paths to those views. The optimized dependency graph remains
 * responsible for unrelated branches of the expanded definition.
 */
@Slf4j
final class ViewDependencyCollector {

  private static final String CTE_RELATION_DEF_CLASS =
      "org.apache.spark.sql.catalyst.plans.logical.CTERelationDef";
  private static final String CTE_RELATION_REF_CLASS =
      "org.apache.spark.sql.catalyst.plans.logical.CTERelationRef";

  private ViewDependencyCollector() {}

  static void collect(ColumnLevelLineageContext context, LogicalPlan analyzedPlan) {
    Map<Long, LogicalPlan> viewCteDefinitions = findViewCteDefinitions(analyzedPlan);
    // Spark plans can share node instances. Cache both true and false results so each exact
    // subtree is inspected once and only ancestors of view-bearing nodes collect dependencies.
    Map<LogicalPlan, ViewPathState> viewPathStates = new IdentityHashMap<>();
    collectViewPaths(context, analyzedPlan, viewPathStates, viewCteDefinitions);
  }

  private static boolean collectViewPaths(
      ColumnLevelLineageContext context,
      LogicalPlan plan,
      Map<LogicalPlan, ViewPathState> viewPathStates,
      Map<Long, LogicalPlan> viewCteDefinitions) {
    ViewPathState state = viewPathStates.get(plan);
    if (state != null) {
      return state == ViewPathState.CONTAINS_VIEW;
    }
    viewPathStates.put(plan, ViewPathState.VISITING);

    boolean viewBoundary = isViewBoundary(plan);

    Optional<Long> cteReferenceId = cteId(plan, CTE_RELATION_REF_CLASS, "cteId");
    if (cteReferenceId.isPresent() && viewCteDefinitions.containsKey(cteReferenceId.get())) {
      // findViewCteDefinitions computes view reachability to a fixed point, so this reference is
      // known to be view-bearing before its definition is traversed. Memoize that fact first to
      // break recursive CTE reference cycles without treating an in-progress path as view-free.
      viewPathStates.put(plan, ViewPathState.CONTAINS_VIEW);
      LogicalPlan definition = viewCteDefinitions.get(cteReferenceId.get());
      addCteReferenceDependencies(context, plan, definition);
      collectViewPaths(context, definition, viewPathStates, viewCteDefinitions);
      return true;
    }

    // A view is part of the path, but it is not a terminal node: its definition may contain nested
    // views whose analyzed expression IDs were removed by optimization.
    boolean foundView = viewBoundary;
    if (plan.children() != null) {
      for (LogicalPlan child : ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children())) {
        // CTE definitions are declarations, not executable branches. A live CTERelationRef follows
        // its definition explicitly above, which prevents unused definitions from adding lineage.
        if (!isCteDefinition(child)) {
          // Deliberately do not short-circuit: every child needs its own memoized result.
          foundView |= collectViewPaths(context, child, viewPathStates, viewCteDefinitions);
        }
      }
    }

    if (foundView) {
      ExpressionDependencyCollector.collectFromOperator(context.getBuilder(), plan);
    }
    viewPathStates.put(
        plan, foundView ? ViewPathState.CONTAINS_VIEW : ViewPathState.CONTAINS_NO_VIEW);
    return foundView;
  }

  private static boolean isViewBoundary(LogicalPlan plan) {
    if (plan instanceof View) {
      return true;
    }
    if (plan instanceof SubqueryAlias && ((SubqueryAlias) plan).child() instanceof View) {
      return true;
    }
    return InputFieldsCollector.isViewSubqueryAlias(plan);
  }

  private static Map<Long, LogicalPlan> findViewCteDefinitions(LogicalPlan analyzedPlan) {
    Map<Long, LogicalPlan> definitions = new HashMap<>();
    collectCteDefinitions(
        analyzedPlan, definitions, Collections.newSetFromMap(new IdentityHashMap<>()));

    Map<Long, Set<Long>> referencedCtes = new HashMap<>();
    Set<Long> viewCteIds = new HashSet<>();
    definitions.forEach(
        (id, definition) -> {
          CteScanResult result = new CteScanResult();
          scanCteDefinition(definition, result, Collections.newSetFromMap(new IdentityHashMap<>()));
          referencedCtes.put(id, result.referencedCteIds);
          if (result.containsView) {
            viewCteIds.add(id);
          }
        });

    boolean changed;
    do {
      changed = false;
      for (Map.Entry<Long, Set<Long>> entry : referencedCtes.entrySet()) {
        if (!viewCteIds.contains(entry.getKey())
            && entry.getValue().stream().anyMatch(viewCteIds::contains)) {
          changed |= viewCteIds.add(entry.getKey());
        }
      }
    } while (changed);

    Map<Long, LogicalPlan> viewDefinitions = new HashMap<>();
    viewCteIds.forEach(id -> viewDefinitions.put(id, definitions.get(id)));
    return viewDefinitions;
  }

  private static void collectCteDefinitions(
      LogicalPlan plan, Map<Long, LogicalPlan> definitions, Set<LogicalPlan> visited) {
    if (!visited.add(plan)) {
      return;
    }

    cteId(plan, CTE_RELATION_DEF_CLASS, "id").ifPresent(id -> definitions.put(id, plan));

    if (plan.children() != null) {
      for (LogicalPlan child : ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children())) {
        collectCteDefinitions(child, definitions, visited);
      }
    }
    for (LogicalPlan subquery : subqueryPlans(plan)) {
      collectCteDefinitions(subquery, definitions, visited);
    }
  }

  private static void scanCteDefinition(
      LogicalPlan plan, CteScanResult result, Set<LogicalPlan> visited) {
    if (!visited.add(plan)) {
      return;
    }
    if (isViewBoundary(plan)) {
      result.containsView = true;
    }

    Optional<Long> referenceId = cteId(plan, CTE_RELATION_REF_CLASS, "cteId");
    if (referenceId.isPresent()) {
      result.referencedCteIds.add(referenceId.get());
      return;
    }

    if (plan.children() != null) {
      for (LogicalPlan child : ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children())) {
        // Nested CTE definitions are declarations. Their live references are recorded from the
        // executable child and propagated through referencedCtes in findViewCteDefinitions.
        if (!isCteDefinition(child)) {
          scanCteDefinition(child, result, visited);
        }
      }
    }
    for (LogicalPlan subquery : subqueryPlans(plan)) {
      scanCteDefinition(subquery, result, visited);
    }
  }

  private static boolean isCteDefinition(LogicalPlan plan) {
    return CTE_RELATION_DEF_CLASS.equals(plan.getClass().getCanonicalName());
  }

  private static Optional<Long> cteId(LogicalPlan plan, String className, String methodName) {
    if (!className.equals(plan.getClass().getCanonicalName())) {
      return Optional.empty();
    }
    try {
      return Optional.of(((Number) plan.getClass().getMethod(methodName).invoke(plan)).longValue());
    } catch (ReflectiveOperationException | ClassCastException e) {
      log.debug("Could not extract CTE ID from {}", className, e);
      return Optional.empty();
    }
  }

  private static void addCteReferenceDependencies(
      ColumnLevelLineageContext context, LogicalPlan reference, LogicalPlan definition) {
    List<Attribute> referenceOutput = ScalaConversionUtils.<Attribute>fromSeq(reference.output());
    List<Attribute> definitionOutput = ScalaConversionUtils.<Attribute>fromSeq(definition.output());
    if (referenceOutput.size() == definitionOutput.size()) {
      for (int i = 0; i < referenceOutput.size(); i++) {
        addAttributeDependency(context, referenceOutput.get(i), definitionOutput.get(i));
      }
      return;
    }

    log.debug(
        "Mapping CTE reference output of size {} to definition output of size {} by name",
        referenceOutput.size(),
        definitionOutput.size());
    for (Attribute referenceAttribute : referenceOutput) {
      Attribute matchingDefinition = null;
      boolean ambiguous = false;
      for (Attribute definitionAttribute : definitionOutput) {
        if (referenceAttribute.name().equals(definitionAttribute.name())) {
          if (matchingDefinition != null) {
            ambiguous = true;
            break;
          }
          matchingDefinition = definitionAttribute;
        }
      }
      if (!ambiguous && matchingDefinition != null) {
        addAttributeDependency(context, referenceAttribute, matchingDefinition);
      } else {
        log.debug(
            "Cannot uniquely map CTE reference output {} to its definition",
            referenceAttribute.name());
      }
    }
  }

  private static void addAttributeDependency(
      ColumnLevelLineageContext context,
      Attribute referenceAttribute,
      Attribute definitionAttribute) {
    if (!referenceAttribute.exprId().equals(definitionAttribute.exprId())) {
      context
          .getBuilder()
          .addDependency(
              referenceAttribute.exprId(),
              definitionAttribute.exprId(),
              TransformationInfo.identity());
    }
  }

  private static List<LogicalPlan> subqueryPlans(LogicalPlan plan) {
    List<LogicalPlan> subqueries = new ArrayList<>();
    if (plan.expressions() != null) {
      collectSubqueryPlans(
          ScalaConversionUtils.<Expression>fromSeq(plan.expressions()), subqueries);
    }
    return subqueries;
  }

  private static void collectSubqueryPlans(
      List<Expression> expressions, List<LogicalPlan> subqueries) {
    for (Expression expression : expressions) {
      if (expression instanceof SubqueryExpression) {
        subqueries.add((LogicalPlan) ((SubqueryExpression) expression).plan());
      }
      if (expression.children() != null) {
        collectSubqueryPlans(
            ScalaConversionUtils.<Expression>fromSeq(expression.children()), subqueries);
      }
    }
  }

  private enum ViewPathState {
    VISITING,
    CONTAINS_VIEW,
    CONTAINS_NO_VIEW
  }

  private static class CteScanResult {
    private boolean containsView;
    private final Set<Long> referencedCteIds = new HashSet<>();
  }
}
