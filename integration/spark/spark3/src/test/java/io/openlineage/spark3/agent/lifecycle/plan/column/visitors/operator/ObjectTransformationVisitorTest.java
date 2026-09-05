/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.CoGroup;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.execution.ExternalRDD;
import org.junit.jupiter.api.Test;

class ObjectTransformationVisitorTest {

  ObjectTransformationVisitor visitor = new ObjectTransformationVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(SerializeFromObject.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testLeafObjectProducerDoesNotThrowAndFallsBackToUnderlyingOutput() {
    // SerializeFromObject -> ExternalRDD (leaf ObjectProducer, NOT a UnaryNode).
    // This is the materialize()/RDD-backed DataFrame case that previously threw
    // ClassCastException (ExternalRDD cannot be cast to UnaryNode), which unwound the whole
    // plan traversal and dropped the entire columnLineage facet for the output dataset.
    ExternalRDD<?> externalRDD = mock(ExternalRDD.class);
    when(externalRDD.output()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1)));

    SerializeFromObject serialize = mock(SerializeFromObject.class);
    when(serialize.child()).thenReturn((LogicalPlan) externalRDD);
    when(serialize.output()).thenReturn(asSeq(field(NAME_2, EXPR_ID_2), field(NAME_3, EXPR_ID_3)));

    assertDoesNotThrow(() -> visitor.apply(serialize, builder));

    // Conservative all-to-all against the leaf's output; InputFieldsCollector attaches the
    // opaque-source identifier for that same exprId.
    verify(builder)
        .addDependency(EXPR_ID_2, EXPR_ID_1, NAME_2, TransformationInfo.transformation());
    verify(builder)
        .addDependency(EXPR_ID_3, EXPR_ID_1, NAME_3, TransformationInfo.transformation());
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testBinaryObjectProducerDoesNotThrow() {
    // SerializeFromObject -> CoGroup (binary ObjectProducer, NOT a UnaryNode).
    CoGroup coGroup = mock(CoGroup.class);
    when(coGroup.output()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1)));

    SerializeFromObject serialize = mock(SerializeFromObject.class);
    when(serialize.child()).thenReturn(coGroup);
    when(serialize.output()).thenReturn(asSeq(field(NAME_2, EXPR_ID_2)));

    assertDoesNotThrow(() -> visitor.apply(serialize, builder));

    verify(builder)
        .addDependency(EXPR_ID_2, EXPR_ID_1, NAME_2, TransformationInfo.transformation());
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testUnaryObjectPipelineDescendsToUnderlyingPlan() {
    // SerializeFromObject -> DeserializeToObject (unary object node) -> base relation.
    // The guard must NOT change this pre-existing behaviour: traversal still descends through
    // the unary object node down to the real data-producing child.
    LogicalPlan baseChild = mock(LogicalPlan.class);
    when(baseChild.output()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1)));

    DeserializeToObject deserialize = mock(DeserializeToObject.class);
    when(deserialize.child()).thenReturn(baseChild);

    SerializeFromObject serialize = mock(SerializeFromObject.class);
    when(serialize.child()).thenReturn((LogicalPlan) deserialize);
    when(serialize.output()).thenReturn(asSeq(field(NAME_2, EXPR_ID_2)));

    visitor.apply(serialize, builder);

    verify(builder)
        .addDependency(EXPR_ID_2, EXPR_ID_1, NAME_2, TransformationInfo.transformation());
    verifyNoMoreInteractions(builder);
  }
}
