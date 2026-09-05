/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef;
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.View;
import org.apache.spark.sql.catalyst.plans.logical.WithCTE;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.immutable.Seq;

class ViewDependencyCollectorTest {

  private final ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  private final ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);

  @BeforeEach
  void setUp() {
    when(context.getBuilder()).thenReturn(builder);
  }

  @Test
  void collectsOnlyDependenciesOnPathsToViews() {
    ExprId viewExprId = ExprId.apply(1);
    ExprId viewOutputExprId = ExprId.apply(2);
    ExprId unrelatedExprId = ExprId.apply(3);
    ExprId unrelatedOutputExprId = ExprId.apply(4);

    View view = mockView(false);

    Project viewProject =
        new Project(
            namedExpressions(alias(viewOutputExprId, "selected", field("view_col", viewExprId))),
            view);

    LogicalPlan unrelatedLeaf = emptyPlan();
    Project unrelatedProject =
        new Project(
            namedExpressions(
                alias(unrelatedOutputExprId, "unrelated", field("unrelated_col", unrelatedExprId))),
            unrelatedLeaf);

    LogicalPlan root = emptyPlan();
    when(root.children())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(viewProject, unrelatedProject)));

    ViewDependencyCollector.collect(context, root);

    verify(builder).addDependency(viewOutputExprId, viewExprId, TransformationInfo.identity());
    verify(builder, never())
        .addDependency(unrelatedOutputExprId, unrelatedExprId, TransformationInfo.identity());
  }

  @Test
  void correlatesCteReferenceWithViewDefinition() {
    ExprId viewExprId = ExprId.apply(1);
    ExprId definitionExprId = ExprId.apply(2);
    ExprId referenceExprId = ExprId.apply(3);
    ExprId outputExprId = ExprId.apply(4);
    long cteId = 100L;

    View view = mockView(false);
    Project definitionProject =
        new Project(
            namedExpressions(alias(definitionExprId, "view_col", field("view_col", viewExprId))),
            view);
    CTERelationDef definition = new CTERelationDef(definitionProject, cteId, Option.empty());

    AttributeReference referenceAttribute = field("view_col", referenceExprId);
    CTERelationRef reference =
        new CTERelationRef(cteId, true, attributes(referenceAttribute), Option.empty());
    Project consumer =
        new Project(
            namedExpressions(alias(outputExprId, "selected", referenceAttribute)), reference);
    WithCTE plan =
        new WithCTE(consumer, ScalaConversionUtils.fromList(Collections.singletonList(definition)));

    assertEquals(cteId, definition.id());
    assertEquals(cteId, reference.cteId());
    assertEquals(definitionExprId, definition.output().apply(0).exprId());
    assertEquals(referenceExprId, reference.output().apply(0).exprId());

    ViewDependencyCollector.collect(context, plan);

    verify(builder).addDependency(outputExprId, referenceExprId, TransformationInfo.identity());
    verify(builder).addDependency(referenceExprId, definitionExprId, TransformationInfo.identity());
    verify(builder).addDependency(definitionExprId, viewExprId, TransformationInfo.identity());
  }

  @Test
  void keepsNestedViewReachableThroughCteWhenOnlyJoinKeySurvives() {
    ExprId leftJoinKeyExprId = ExprId.apply(1);
    ExprId leftOutputExprId = ExprId.apply(2);
    ExprId sourceExprId = ExprId.apply(3);
    ExprId definitionExprId = ExprId.apply(4);
    ExprId referenceExprId = ExprId.apply(5);
    ExprId nestedViewExprId = ExprId.apply(6);
    ExprId finalOutputExprId = ExprId.apply(7);
    long cteId = 100L;

    View sourceView = mockView(false);
    Project definitionProject =
        new Project(
            namedExpressions(alias(definitionExprId, "ptp_id", field("ptp_id", sourceExprId))),
            sourceView);
    CTERelationDef definition = new CTERelationDef(definitionProject, cteId, Option.empty());

    AttributeReference referenceAttribute = field("ptp_id", referenceExprId);
    CTERelationRef reference =
        new CTERelationRef(cteId, true, attributes(referenceAttribute), Option.empty());
    Project cteConsumer =
        new Project(
            namedExpressions(alias(nestedViewExprId, "ptp_id", referenceAttribute)), reference);
    WithCTE nestedViewDefinition =
        new WithCTE(
            cteConsumer, ScalaConversionUtils.fromList(Collections.singletonList(definition)));
    View nestedView = mockView(false, nestedViewDefinition);

    Join join =
        new Join(
            emptyPlan(),
            nestedView,
            JoinType.apply("inner"),
            ScalaConversionUtils.toScalaOption(
                (Expression)
                    new EqualTo(
                        field("left_ptp_id", leftJoinKeyExprId),
                        field("ptp_id", nestedViewExprId))),
            JoinHint.NONE());
    // The outer view does not project any nested-view payload; only the join key remains reachable.
    Project outerProject =
        new Project(
            namedExpressions(
                alias(finalOutputExprId, "payload", field("payload", leftOutputExprId))),
            join);
    View outerView = mockView(false, outerProject);

    assertEquals(nestedViewExprId, nestedView.output().apply(0).exprId());

    ViewDependencyCollector.collect(context, outerView);

    verify(builder).addDatasetDependency(any(ExprId.class));
    verify(builder)
        .addDependency(
            any(ExprId.class),
            eq(nestedViewExprId),
            eq(TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN)));
    verify(builder).addDependency(nestedViewExprId, referenceExprId, TransformationInfo.identity());
    verify(builder).addDependency(referenceExprId, definitionExprId, TransformationInfo.identity());
    verify(builder).addDependency(definitionExprId, sourceExprId, TransformationInfo.identity());
  }

  @Test
  void doesNotOverattributeConditionlessJoinFields() {
    ExprId leftOutputExprId = ExprId.apply(1);
    ExprId nestedViewExprId = ExprId.apply(2);
    ExprId finalOutputExprId = ExprId.apply(3);

    AttributeReference leftOutput = field("payload", leftOutputExprId);
    LogicalPlan leftPlan = emptyPlan();
    when(leftPlan.output()).thenReturn(attributes(leftOutput));
    View nestedView = mockView(false);
    when(nestedView.output()).thenReturn(attributes(field("nested_value", nestedViewExprId)));
    Join crossJoin =
        new Join(leftPlan, nestedView, JoinType.apply("cross"), Option.empty(), JoinHint.NONE());
    Project outerProject =
        new Project(namedExpressions(alias(finalOutputExprId, "payload", leftOutput)), crossJoin);

    ViewDependencyCollector.collect(context, outerProject);

    verify(builder, never()).addDatasetDependency(any(ExprId.class));
    verify(builder, never())
        .addDependency(any(ExprId.class), eq(nestedViewExprId), any(TransformationInfo.class));
  }

  @Test
  void ignoresUnreferencedViewBearingCteDefinition() {
    ExprId sourceExprId = ExprId.apply(1);
    ExprId definitionExprId = ExprId.apply(2);
    long cteId = 100L;

    Project definitionProject =
        new Project(
            namedExpressions(alias(definitionExprId, "unused", field("unused", sourceExprId))),
            mockView(false));
    CTERelationDef unusedDefinition = new CTERelationDef(definitionProject, cteId, Option.empty());
    WithCTE plan =
        new WithCTE(
            emptyPlan(),
            ScalaConversionUtils.fromList(Collections.singletonList(unusedDefinition)));

    ViewDependencyCollector.collect(context, plan);

    verifyNoInteractions(builder);
  }

  @Test
  void terminatesWhenViewBearingCteReferencesItself() {
    ExprId selfReferenceExprId = ExprId.apply(1);
    ExprId definitionExprId = ExprId.apply(2);
    ExprId consumerReferenceExprId = ExprId.apply(3);
    ExprId consumerOutputExprId = ExprId.apply(4);
    ExprId viewSourceExprId = ExprId.apply(5);
    ExprId viewOutputExprId = ExprId.apply(6);
    long cteId = 100L;

    AttributeReference selfReferenceAttribute = field("value", selfReferenceExprId);
    CTERelationRef selfReference =
        new CTERelationRef(cteId, true, attributes(selfReferenceAttribute), Option.empty());
    Project viewProject =
        new Project(
            namedExpressions(
                alias(viewOutputExprId, "view_value", field("value", viewSourceExprId))),
            mockView(false));
    LogicalPlan definitionBody = emptyPlan();
    when(definitionBody.children())
        .thenReturn(ScalaConversionUtils.fromList(Arrays.asList(selfReference, viewProject)));
    when(definitionBody.output()).thenReturn(attributes(field("value", definitionExprId)));
    when(definitionBody.resolved()).thenReturn(true);
    CTERelationDef definition = new CTERelationDef(definitionBody, cteId, Option.empty());

    AttributeReference consumerReferenceAttribute = field("value", consumerReferenceExprId);
    CTERelationRef consumerReference =
        new CTERelationRef(cteId, true, attributes(consumerReferenceAttribute), Option.empty());
    Project consumer =
        new Project(
            namedExpressions(alias(consumerOutputExprId, "value", consumerReferenceAttribute)),
            consumerReference);
    WithCTE plan =
        new WithCTE(consumer, ScalaConversionUtils.fromList(Collections.singletonList(definition)));

    assertDoesNotThrow(() -> ViewDependencyCollector.collect(context, plan));

    verify(builder)
        .addDependency(consumerReferenceExprId, definitionExprId, TransformationInfo.identity());
    verify(builder)
        .addDependency(selfReferenceExprId, definitionExprId, TransformationInfo.identity());
    verify(builder)
        .addDependency(viewOutputExprId, viewSourceExprId, TransformationInfo.identity());
  }

  @Test
  void ignoresNestedUnreferencedViewBearingCteInsideReferencedCte() {
    ExprId innerSourceExprId = ExprId.apply(1);
    ExprId innerDefinitionExprId = ExprId.apply(2);
    ExprId outerSourceExprId = ExprId.apply(3);
    ExprId outerDefinitionExprId = ExprId.apply(4);
    ExprId outerReferenceExprId = ExprId.apply(5);
    ExprId finalOutputExprId = ExprId.apply(6);
    long innerCteId = 100L;
    long outerCteId = 200L;

    Project innerDefinitionProject =
        new Project(
            namedExpressions(
                alias(innerDefinitionExprId, "unused", field("unused", innerSourceExprId))),
            mockView(false));
    CTERelationDef innerDefinition =
        new CTERelationDef(innerDefinitionProject, innerCteId, Option.empty());

    Project outerBody =
        new Project(
            namedExpressions(
                alias(outerDefinitionExprId, "base", field("base", outerSourceExprId))),
            emptyPlan());
    WithCTE outerDefinitionPlan =
        new WithCTE(
            outerBody, ScalaConversionUtils.fromList(Collections.singletonList(innerDefinition)));
    CTERelationDef outerDefinition =
        new CTERelationDef(outerDefinitionPlan, outerCteId, Option.empty());

    AttributeReference outerReferenceAttribute = field("base", outerReferenceExprId);
    CTERelationRef outerReference =
        new CTERelationRef(outerCteId, true, attributes(outerReferenceAttribute), Option.empty());
    Project consumer =
        new Project(
            namedExpressions(alias(finalOutputExprId, "base", outerReferenceAttribute)),
            outerReference);
    WithCTE plan =
        new WithCTE(
            consumer, ScalaConversionUtils.fromList(Collections.singletonList(outerDefinition)));

    ViewDependencyCollector.collect(context, plan);

    verifyNoInteractions(builder);
  }

  @Test
  void mapsMatchingCteOutputsWhenReferenceIsPruned() {
    ExprId sourceJoinKeyExprId = ExprId.apply(1);
    ExprId sourcePayloadExprId = ExprId.apply(2);
    ExprId definitionJoinKeyExprId = ExprId.apply(3);
    ExprId definitionPayloadExprId = ExprId.apply(4);
    ExprId referencePayloadExprId = ExprId.apply(5);
    ExprId finalOutputExprId = ExprId.apply(6);
    long cteId = 100L;

    Project definitionProject =
        new Project(
            namedExpressions(
                alias(definitionJoinKeyExprId, "join_key", field("join_key", sourceJoinKeyExprId)),
                alias(definitionPayloadExprId, "payload", field("payload", sourcePayloadExprId))),
            mockView(false));
    CTERelationDef definition = new CTERelationDef(definitionProject, cteId, Option.empty());

    AttributeReference referencePayload = field("payload", referencePayloadExprId);
    CTERelationRef prunedReference =
        new CTERelationRef(cteId, true, attributes(referencePayload), Option.empty());
    Project consumer =
        new Project(
            namedExpressions(alias(finalOutputExprId, "payload", referencePayload)),
            prunedReference);
    WithCTE plan =
        new WithCTE(consumer, ScalaConversionUtils.fromList(Collections.singletonList(definition)));

    ViewDependencyCollector.collect(context, plan);

    verify(builder)
        .addDependency(
            referencePayloadExprId, definitionPayloadExprId, TransformationInfo.identity());
  }

  @Test
  void collectsTemporaryViewWrappedInSubqueryAlias() {
    ExprId viewExprId = ExprId.apply(1);
    ExprId outputExprId = ExprId.apply(2);

    View temporaryView = mockView(true);
    SubqueryAlias alias =
        new SubqueryAlias(
            new AliasIdentifier(
                "temporary_view",
                ScalaConversionUtils.fromList(Collections.singletonList("global_temp"))),
            temporaryView);
    Project consumer =
        new Project(
            namedExpressions(alias(outputExprId, "selected", field("view_col", viewExprId))),
            alias);

    ViewDependencyCollector.collect(context, consumer);

    verify(builder).addDependency(outputExprId, viewExprId, TransformationInfo.identity());
  }

  @Test
  void doesNothingWhenPlanContainsNoView() {
    ExprId inputExprId = ExprId.apply(1);
    ExprId outputExprId = ExprId.apply(2);
    Project project =
        new Project(
            namedExpressions(alias(outputExprId, "selected", field("input", inputExprId))),
            emptyPlan());

    ViewDependencyCollector.collect(context, project);

    verifyNoInteractions(builder);
  }

  private static View mockView(boolean temporary) {
    View view = mock(View.class);
    when(view.isTempView()).thenReturn(temporary);
    when(view.resolved()).thenReturn(true);
    when(view.children()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    when(view.expressions()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    return view;
  }

  private static View mockView(boolean temporary, LogicalPlan child) {
    View view = mockView(temporary);
    when(view.child()).thenReturn(child);
    when(view.children())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(child)));
    when(view.output()).thenReturn(child.output());
    return view;
  }

  private static LogicalPlan emptyPlan() {
    LogicalPlan plan = mock(LogicalPlan.class);
    when(plan.children()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    when(plan.expressions()).thenReturn(ScalaConversionUtils.fromList(Collections.emptyList()));
    return plan;
  }

  private static AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static Alias alias(ExprId exprId, String name, Expression child) {
    return new Alias(
        child,
        name,
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty(),
        Option.empty(),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static Seq<NamedExpression> namedExpressions(NamedExpression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.asList(expressions));
  }

  private static Seq<Attribute> attributes(Attribute... attributes) {
    return ScalaConversionUtils.fromList(Arrays.asList(attributes));
  }
}
