/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_4;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

class ExpandVisitorTest {

  ExpandVisitor visitor = new ExpandVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  // Source attribute (original exprId from analyzed plan)
  Attribute sourceAttr = field("col", EXPR_ID_1);
  // Expanded attribute (fresh exprId introduced by DistinctAggregationRewriter)
  Attribute expandedAttr = field("col_exp", EXPR_ID_2);
  // gid column (synthetic, no source)
  Attribute gidAttr = field("gid", EXPR_ID_3);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Expand.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testNonNullProjectionCreatesEdge() {
    // Single COUNT(DISTINCT col): one slot has the real column, the other has typed null
    //   projections = [[col#1L, Literal(0)], [Literal(null), Literal(1)]]
    //   output      = [col_exp#2L, gid#3L]
    Literal nullLit = new Literal(null, IntegerType$.MODULE$);
    Literal gid0 = new Literal(0, IntegerType$.MODULE$);
    Literal gid1 = new Literal(1, IntegerType$.MODULE$);

    Seq<Expression> row0 = seq(sourceAttr, gid0);
    Seq<Expression> row1 = seq(nullLit, gid1);
    Seq<Seq<Expression>> projections = seq(row0, row1);
    Seq<Attribute> output = seq(expandedAttr, gidAttr);

    Expand expand = new Expand(projections, output, mock(LogicalPlan.class));
    visitor.apply(expand, builder);

    // col_exp#2L depends on col#1L (from row0)
    verify(builder).addDependency(EXPR_ID_2, EXPR_ID_1, TransformationInfo.identity());
    // gid has only literals — no column dependencies
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testTypedNullIsSkipped() {
    // Typed null literal (Literal(null, LongType)) must be treated as a no-lineage slot
    Literal typedNull = new Literal(null, LongType$.MODULE$);
    Literal gid0 = new Literal(0, IntegerType$.MODULE$);

    Seq<Expression> row0 = seq(sourceAttr, gid0);
    Seq<Expression> row1 = seq(typedNull, gid0);
    Seq<Seq<Expression>> projections = seq(row0, row1);
    Seq<Attribute> output = seq(expandedAttr, gidAttr);

    Expand expand = new Expand(projections, output, mock(LogicalPlan.class));
    visitor.apply(expand, builder);

    verify(builder).addDependency(EXPR_ID_2, EXPR_ID_1, TransformationInfo.identity());
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testMultipleDistinctAggregatesNoContamination() {
    // Two COUNT(DISTINCT) on different columns — the Expand pattern that triggered this fix.
    //   COUNT(DISTINCT col_a), COUNT(DISTINCT col_b) GROUP BY key
    //   projections = [
    //     [col_a#1L, null,    Literal(0)],   // gid=0: first COUNT DISTINCT slot
    //     [null,    col_b#3L, Literal(1)],   // gid=1: second COUNT DISTINCT slot
    //   ]
    //   output = [col_a_exp#2L, col_b_exp#4L, gid#?]
    Attribute colA = field("col_a", EXPR_ID_1);
    Attribute colAExp = field("col_a_exp", EXPR_ID_2);
    Attribute colB = field("col_b", EXPR_ID_3);
    Attribute colBExp = field("col_b_exp", EXPR_ID_4);

    Literal nullLit = new Literal(null, IntegerType$.MODULE$);
    Literal gid0 = new Literal(0, IntegerType$.MODULE$);
    Literal gid1 = new Literal(1, IntegerType$.MODULE$);

    Seq<Expression> row0 = seq(colA, nullLit, gid0);
    Seq<Expression> row1 = seq(nullLit, colB, gid1);
    Seq<Seq<Expression>> projections = seq(row0, row1);
    Seq<Attribute> output = seq(colAExp, colBExp, gidAttr);

    Expand expand = new Expand(projections, output, mock(LogicalPlan.class));
    visitor.apply(expand, builder);

    // Each expanded attr maps to its own source — no cross-contamination
    verify(builder).addDependency(EXPR_ID_2, EXPR_ID_1, TransformationInfo.identity());
    verify(builder).addDependency(EXPR_ID_4, EXPR_ID_3, TransformationInfo.identity());
    verifyNoMoreInteractions(builder);
  }

  @SafeVarargs
  private static <T> Seq<T> seq(T... items) {
    return ScalaConversionUtils.fromList(Arrays.asList(items));
  }
}
