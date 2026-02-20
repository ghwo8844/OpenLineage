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

/**
 * Visitor that extracts lineage from Spark {@link GetMapValue} expressions.
 *
 * <p>For map access expressions like {@code map_col['key']}, this visitor tracks both the
 * dependency on the map column itself and captures the specific key being accessed for enhanced
 * transformation tracking.
 */
public class GetMapValueVisitor implements ExpressionVisitor {

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof GetMapValue;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    GetMapValue expr = (GetMapValue) expression;
    
    // Track dependency on the map column itself
    traverser.copyFor(expr.child(), TransformationInfo.transformation()).traverse();
    
    // If the key is a literal, we can enhance the transformation info with the map key
    if (expr.key() instanceof Literal) {
      Literal keyLiteral = (Literal) expr.key();
      String mapKey = keyLiteral.value() != null ? keyLiteral.value().toString() : "null";
      
      // Create transformation info that includes the map key in description
      TransformationInfo mapKeyTransformation = TransformationInfo.transformation(
          "map_key_access[" + mapKey + "]"
      );
      
      // Add dependency with map key information
      traverser.copyOverrideTransform(expr.child(), mapKeyTransformation).traverse();
    } else {
      // If key is dynamic, just track it as a transformation dependency
      traverser.copyFor(expr.key(), TransformationInfo.transformation()).traverse();
    }
  }
}
