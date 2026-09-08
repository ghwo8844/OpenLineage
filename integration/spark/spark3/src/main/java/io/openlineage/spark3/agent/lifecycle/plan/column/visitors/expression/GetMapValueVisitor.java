/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetMapValue;
import org.apache.spark.sql.catalyst.expressions.Literal;

public class GetMapValueVisitor implements ExpressionVisitor {

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof GetMapValue;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    GetMapValue expr = (GetMapValue) expression;

    // Step 1: record the base column dependency without carrying any outer fieldPath,
    // so intermediate access levels are captured cleanly.
    traverser.copyForStrippingFieldPath(expr.child()).traverse();

    // Step 2: record with the full access path using Spark's native SQL rendering, which
    // handles all literal types (strings are quoted, nulls are NULL, dates are formatted, etc.).
    traverser.copyWithFieldPath(expr.child(), "[" + expr.key().sql() + "]").traverse();

    // Step 3: for dynamic (non-literal) keys, also record a dependency on the key expression
    // itself so the column driving the lookup is captured in lineage.
    if (!(expr.key() instanceof Literal)) {
      traverser.copyFor(expr.key(), TransformationInfo.transformation()).traverse();
    }
  }
}
