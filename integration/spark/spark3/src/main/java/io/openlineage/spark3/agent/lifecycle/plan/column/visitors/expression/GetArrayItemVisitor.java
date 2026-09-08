/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayItem;

public class GetArrayItemVisitor implements ExpressionVisitor {

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof GetArrayItem;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    GetArrayItem expr = (GetArrayItem) expression;

    // Step 1: record the base column dependency without outer fieldPath leaking in.
    traverser.copyForStrippingFieldPath(expr.child()).traverse();

    // Step 2: record with the full access path. The actual index is not exposed (always "[0]")
    // to avoid leaking data size or positional information in lineage output.
    traverser.copyWithFieldPath(expr.child(), "[0]").traverse();
  }
}
