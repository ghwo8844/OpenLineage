/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ObjectConsumer;
import org.apache.spark.sql.catalyst.plans.logical.ObjectProducer;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

/**
 * Handles typed Dataset API operations (flatMap, map, mapPartitions) that produce
 * SerializeFromObject → MapElements → DeserializeToObject plan chains.
 *
 * <p>Since user-defined functions cannot be introspected, this visitor creates all-to-all
 * dependencies: every output column depends on every input column of the underlying plan.
 */
public class ObjectTransformationVisitor implements OperatorVisitor {

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof SerializeFromObject;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    // Walk past the object pipeline (SerializeFromObject → MapElements → DeserializeToObject)
    // to find the actual data-producing child plan.
    //
    // Only descend while the node is a UnaryNode. An object node can bottom out in a non-unary
    // node — a leaf ObjectProducer (e.g. ExternalRDD from a materialized/RDD-backed DataFrame) or
    // a binary ObjectProducer (e.g. CoGroup). Casting those to UnaryNode throws
    // ClassCastException.
    LogicalPlan current = ((UnaryNode) operator).child();
    while ((current instanceof ObjectConsumer || current instanceof ObjectProducer)
        && current instanceof UnaryNode) {
      current = ((UnaryNode) current).child();
    }

    List<Attribute> outputAttrs = ScalaConversionUtils.fromSeq(operator.output());
    List<Attribute> inputAttrs = ScalaConversionUtils.fromSeq(current.output());

    // Create all-to-all dependencies: each output depends on every input
    for (Attribute output : outputAttrs) {
      for (Attribute input : inputAttrs) {
        builder.addDependency(output.exprId(), input.exprId(), TransformationInfo.transformation());
      }
    }
  }
}
