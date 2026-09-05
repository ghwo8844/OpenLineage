/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Connects {@link Expand} output attributes to their source expressions so that lineage survives
 * the two-phase aggregate rewrite Spark's {@code DistinctAggregationRewriter} applies to {@code
 * COUNT(DISTINCT ...)}. Also fixes lineage for {@code CUBE}, {@code ROLLUP}, and {@code GROUPING
 * SETS}, which use the same {@code Expand} mechanism.
 *
 * <p>{@code Expand} projects each input row into multiple output rows, one per distinct group.
 * Slots that do not belong to a given group are filled with null. For each output attribute at
 * index {@code j}, the contributing expressions are the non-null entries of {@code
 * projections[k][j]} across all projection rows {@code k}.
 */
public class ExpandVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Expand;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Expand expand = (Expand) operator;
    List<Attribute> output = ScalaConversionUtils.fromSeq(expand.output());
    List<List<Expression>> projections =
        ScalaConversionUtils.fromSeq(expand.projections()).stream()
            .map(row -> ScalaConversionUtils.<Expression>fromSeq(row))
            .collect(Collectors.toList());

    for (int j = 0; j < output.size(); j++) {
      Attribute attr = output.get(j);
      final int col = j;
      projections.stream()
          .filter(row -> row.size() == output.size()) // guard against malformed Expand nodes
          .map(row -> row.get(col))
          .filter(expr -> !isNullExpression(expr))
          .forEach(
              expr ->
                  ExpressionTraverser.of(
                          expr, attr.exprId(), TransformationInfo.identity(), builder)
                      .traverse());
    }
  }

  /**
   * Returns true for expressions that carry no lineage: plain null literals, typed null literals
   * (e.g. {@code Literal(null, LongType)}), and casts of null literals (e.g. {@code Cast(null,
   * LongType)}).
   */
  private static boolean isNullExpression(Expression expr) {
    if (expr instanceof Literal) {
      return ((Literal) expr).value() == null;
    }
    if (expr instanceof Cast) {
      return isNullExpression(((Cast) expr).child());
    }
    return false;
  }
}
