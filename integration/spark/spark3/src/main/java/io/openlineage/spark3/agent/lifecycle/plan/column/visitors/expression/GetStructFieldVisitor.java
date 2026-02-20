/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetStructField;

/**
 * Visitor that extracts lineage from Spark {@link GetStructField} expressions.
 *
 * <p>For struct field access expressions like {@code struct_col.field_name}, this visitor tracks
 * both the dependency on the struct column itself and captures the specific field being accessed
 * for enhanced transformation tracking.
 */
public class GetStructFieldVisitor implements ExpressionVisitor {

  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof GetStructField;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    GetStructField expr = (GetStructField) expression;
    
    // Track dependency on the struct column itself
    traverser.copyFor(expr.child(), TransformationInfo.transformation()).traverse();
    
    // GetStructField always has a field name, extract it
    String fieldName = expr.childSchema().apply(expr.ordinal()).name();
    
    // Create transformation info that includes the struct field in description
    TransformationInfo structFieldTransformation = TransformationInfo.transformation(
        "struct_field_access[" + fieldName + "]"
    );
    
    // Add dependency with struct field information
    traverser.copyOverrideTransform(expr.child(), structFieldTransformation).traverse();
  }
}
