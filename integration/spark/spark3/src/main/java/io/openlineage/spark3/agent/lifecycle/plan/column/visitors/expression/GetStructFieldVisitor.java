/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetStructField;

public class GetStructFieldVisitor implements ExpressionVisitor {

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof GetStructField;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    GetStructField expr = (GetStructField) expression;

    // Step 1: record the base column dependency without carrying any outer fieldPath.
    traverser.copyForStrippingFieldPath(expr.child()).traverse();

    // Step 2: record with the full access path, accumulating outer fieldPath if present.
    String fieldName = expr.childSchema().apply(expr.ordinal()).name();
    traverser.copyWithFieldPath(expr.child(), "." + fieldName).traverse();
  }
}
