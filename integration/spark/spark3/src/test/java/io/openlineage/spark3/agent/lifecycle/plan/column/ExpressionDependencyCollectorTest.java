/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayItem;
import org.apache.spark.sql.catalyst.expressions.GetMapValue;
import org.apache.spark.sql.catalyst.expressions.GetStructField;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.NullOrdering;
import org.apache.spark.sql.catalyst.expressions.Sha1;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import scala.Option;
import scala.collection.immutable.Seq;

class ExpressionDependencyCollectorTest {

  final String ALIAS_NAME = "res";
  final String NAME1 = "name1";
  final String NAME2 = "name2";
  final String NAME3 = "name3";
  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);
  ExprId exprId1 = ExprId.apply(21);
  ExprId exprId2 = ExprId.apply(22);
  ExprId exprId3 = ExprId.apply(23);
  ExprId exprId4 = ExprId.apply(24);
  ExprId exprId5 = ExprId.apply(25);

  NamedExpression expression1 = field(NAME1, exprId1);
  NamedExpression expression2 = field(NAME2, exprId2);

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(mock(OpenLineageContext.class));
    exprIdAccumulator.reset();
  }

  @Test
  void CollectFromComplexPlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);

      AttributeReference expression3 = field(NAME3, exprId3);
      EqualTo equalTo = new EqualTo((Expression) expression1, (Expression) expression2);
      GreaterThan greaterThan = new GreaterThan(expression3, new Literal(5, IntegerType$.MODULE$));

      Join join =
          new Join(
              mock(LogicalPlan.class),
              mock(LogicalPlan.class),
              JoinType.apply("inner"),
              ScalaConversionUtils.toScalaOption((Expression) equalTo),
              JoinHint.NONE());
      Filter filter = new Filter(new And(equalTo, greaterThan), join);
      Sort sort =
          new Sort(
              ScalaConversionUtils.fromList(
                  Collections.singletonList(
                      new SortOrder(
                          (Expression) expression1,
                          mock(SortDirection.class),
                          mock(NullOrdering.class),
                          ScalaConversionUtils.asScalaSeqEmpty()))),
              true,
              filter);

      LogicalPlan plan = new CreateTableAsSelect(null, null, null, sort, null, null, false);
      ExpressionDependencyCollector.collect(context, plan);

      String sortColumns = "name1 null null";
      String joinCondition = "(name1 = name2)";
      String filterCondition = "(" + joinCondition + " AND (name3 > 5))";
      verify(builder, times(1)).addDatasetDependency(ExprId.apply(0), "SORT BY " + sortColumns);

      verify(builder, times(1)).addDatasetDependency(ExprId.apply(1), "WHERE " + filterCondition);
      verify(builder, times(1))
          .addDatasetDependency(ExprId.apply(2), "INNER JOIN ON " + joinCondition);

      verify(builder, times(1))
          .addDependency(
              ExprId.apply(0),
              exprId1,
              sortColumns,
              TransformationInfo.indirect(TransformationInfo.Subtypes.SORT, sortColumns));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId1,
              filterCondition,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER, filterCondition));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId2,
              filterCondition,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER, filterCondition));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId3,
              filterCondition,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER, filterCondition));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(2),
              exprId1,
              joinCondition,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN, joinCondition));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(2),
              exprId2,
              joinCondition,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN, joinCondition));

      utilities.verify(NamedExpression::newExprId, times(3));
    }
  }

  @Test
  void testCollectIFExpressions() {
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            field(NAME3, exprId3),
            field("name4", exprId4));
    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    String description = "(IF((name1 = name2), name3, name4)) AS res";
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId2,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId3, ALIAS_NAME, TransformationInfo.transformation(description));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId4, ALIAS_NAME, TransformationInfo.transformation(description));
  }

  @Test
  void testCollectMultipleDirectTransformationsForOneInput() {
    AttributeReference expression3 = field(NAME3, exprId3);
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            expression3,
            new Add(expression3, new Literal(1, IntegerType$.MODULE$)));
    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    String description = "(IF((name1 = name2), name3, (name3 + 1))) AS res";
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId2,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(2))
        .addDependency(
            exprId5, exprId3, ALIAS_NAME, TransformationInfo.transformation(description));
  }

  @Test
  void testCollectCaseWhenExpressions() {
    CaseWhen caseWhen =
        new CaseWhen(
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    ScalaConversionUtils.toScalaTuple(
                        (Expression) expression1, (Expression) expression2))),
            ScalaConversionUtils.toScalaOption((Expression) field(NAME3, exprId3)));
    Alias res = alias(exprId4, ALIAS_NAME, caseWhen);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    String description = "CASE WHEN name1 THEN name2 ELSE name3 END AS res";
    verify(builder, times(1))
        .addDependency(
            exprId4,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId4, exprId2, ALIAS_NAME, TransformationInfo.transformation(description));
    verify(builder, times(1))
        .addDependency(
            exprId4, exprId3, ALIAS_NAME, TransformationInfo.transformation(description));
  }

  @Test
  void testCollectMaskingExpressions() {
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            field(NAME3, exprId3),
            new Sha1(field("name4", exprId4)));

    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);

    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    String description = "(IF((name1 = name2), name3, sha1(name4))) AS res";
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId5,
            exprId2,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId3, ALIAS_NAME, TransformationInfo.transformation(description));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId4, ALIAS_NAME, TransformationInfo.transformation(description, true));
  }

  @Test
  void testCollectTraversingExpressions() {
    // AggregateExpression - mock aggregate functions
    AggregateFunction aggrFunc1 = mock(AggregateFunction.class);
    AggregateFunction aggrFunc2 = mock(AggregateFunction.class);

    when(aggrFunc1.sql()).thenReturn("aggr1_func_sql");
    when(aggrFunc2.sql()).thenReturn("aggr2_func_sql");

    AggregateExpression aggr1 = mock(AggregateExpression.class);
    AggregateExpression aggr2 = mock(AggregateExpression.class);

    when(aggr1.resultId()).thenReturn(exprId1);
    when(aggr2.resultId()).thenReturn(exprId2);
    when(aggr1.sql()).thenReturn("aggr1_sql");
    when(aggr2.sql()).thenReturn("aggr2_sql");
    when(aggr1.aggregateFunction()).thenReturn(aggrFunc1);
    when(aggr2.aggregateFunction()).thenReturn(aggrFunc2);

    // BinaryExpression
    BinaryExpression binaryExpression = mock(BinaryExpression.class);
    doReturn(ScalaConversionUtils.fromList(Arrays.asList((Expression) aggr1, aggr2)))
        .when(binaryExpression)
        .children();
    when(binaryExpression.sql()).thenReturn("binary_expr_sql");

    ExprId rootAliasExprId = mock(ExprId.class);
    Alias rootAlias = alias(rootAliasExprId, NAME2, (Expression) binaryExpression);

    Project project =
        new Project(
            ScalaConversionUtils.fromList(Collections.singletonList(rootAlias)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            rootAliasExprId,
            exprId1,
            "name2",
            TransformationInfo.aggregation("binary_expr_sql AS name2"));
  }

  @Test
  void testCollectCoalesceExpressions() {
    Coalesce coalesceExpr =
        new Coalesce(getExpressionSeq((Expression) expression1, (Expression) expression2));
    Alias res = alias(exprId3, ALIAS_NAME, coalesceExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    String description = "coalesce(name1, name2) AS res";
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId2,
            ALIAS_NAME,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, times(1))
        .addDependency(
            exprId3, exprId1, ALIAS_NAME, TransformationInfo.transformation(description));
    verify(builder, times(1))
        .addDependency(
            exprId3, exprId2, ALIAS_NAME, TransformationInfo.transformation(description));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testCollectCoalesceWithLiteralFallback() {
    // COALESCE(col, 0) — common pattern after LEFT JOIN where the literal default has no lineage
    Coalesce coalesceExpr =
        new Coalesce(
            getExpressionSeq((Expression) expression1, new Literal(0, IntegerType$.MODULE$)));
    Alias res = alias(exprId3, ALIAS_NAME, coalesceExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.indirect(
                TransformationInfo.Subtypes.CONDITIONAL, "coalesce(name1, 0) AS res"));
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            TransformationInfo.transformation("coalesce(name1, 0) AS res"));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testNestedMapAccessBuildsCumulativeFieldPath() {
    // mapcol['key1']['innerkey1'] — AST: GetMapValue(GetMapValue(mapcol, key1), innerkey1)
    // mapcol type: Map<String, Map<String, String>> to match two-level access
    AttributeReference mapcol =
        new AttributeReference(
            "mapcol",
            DataTypes.createMapType(
                DataTypes.StringType,
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
            false,
            Metadata$.MODULE$.empty(),
            exprId1,
            ScalaConversionUtils.asScalaSeqEmpty());
    Literal key1 = new Literal(UTF8String.fromString("key1"), StringType$.MODULE$);
    Literal innerkey1 = new Literal(UTF8String.fromString("innerkey1"), StringType$.MODULE$);
    GetMapValue innerAccess = new GetMapValue(mapcol, key1);
    GetMapValue outerAccess = new GetMapValue(innerAccess, innerkey1);

    Alias alias = alias(exprId3, ALIAS_NAME, outerAccess);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    // Intermediate access: ['key1']
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "mapcol['key1']['innerkey1'] AS res",
                false,
                "['key1']"));
    // Full nested path: ['key1']['innerkey1']
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "mapcol['key1']['innerkey1'] AS res",
                false,
                "['key1']['innerkey1']"));
    // The spurious ['innerkey1']-only entry must NOT appear
    verify(builder, Mockito.never())
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "mapcol['key1']['innerkey1'] AS res",
                false,
                "['innerkey1']"));
  }

  @Test
  void testNestedStructAccessBuildsCumulativeFieldPath() {
    // structcol.field1.field2 — AST: GetStructField(GetStructField(structcol, 0/*field1*/),
    // 0/*field2*/)
    StructType innerType =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("field2", DataTypes.StringType, true)});
    StructType outerType =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("field1", innerType, true)});
    AttributeReference structcol = structField("structcol", outerType, exprId1);
    GetStructField getField1 = new GetStructField(structcol, 0, Option.empty());
    GetStructField getField2 = new GetStructField(getField1, 0, Option.empty());

    Alias alias = alias(exprId3, ALIAS_NAME, getField2);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    // Intermediate access: .field1
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "structcol.`field1`.`field2` AS res",
                false,
                ".field1"));
    // Full nested path: .field1.field2
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "structcol.`field1`.`field2` AS res",
                false,
                ".field1.field2"));
    // The spurious .field2-only entry must NOT appear
    verify(builder, Mockito.never())
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "structcol.`field1`.`field2` AS res",
                false,
                ".field2"));
  }

  @Test
  void testCrossTypeNestedAccessMapThenStruct() {
    // mapcol['key1'].field — AST: GetStructField(GetMapValue(mapcol_map_to_struct, key1), 0)
    StructType valueType =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("field", DataTypes.StringType, true)});
    // mapcol has type Map<String, StructType{field: String}>
    AttributeReference mapcol =
        new AttributeReference(
            "mapcol",
            DataTypes.createMapType(DataTypes.StringType, valueType),
            false,
            Metadata$.MODULE$.empty(),
            exprId1,
            ScalaConversionUtils.asScalaSeqEmpty());
    Literal key1 = new Literal(UTF8String.fromString("key1"), StringType$.MODULE$);
    // mapAccess.dataType = valueType (StructType) — childSchema() will resolve "field"
    GetMapValue mapAccess = new GetMapValue(mapcol, key1);
    GetStructField structAccess = new GetStructField(mapAccess, 0, Option.empty());

    Alias alias = alias(exprId3, ALIAS_NAME, structAccess);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    // Intermediate: ['key1'] from the map access
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "mapcol['key1'].`field` AS res",
                false,
                "['key1']"));
    // Full cross-type path: ['key1'].field
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "mapcol['key1'].`field` AS res",
                false,
                "['key1'].field"));
  }

  @Test
  void testMapAccessWithDynamicColumnKeyRecordsColumnName() {
    // scores[key_col] — dynamic key: fieldPath = [key_col] via expr.key().sql(), plus dependency on
    // key_col
    AttributeReference scores =
        new AttributeReference(
            "scores",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
            false,
            Metadata$.MODULE$.empty(),
            exprId1,
            ScalaConversionUtils.asScalaSeqEmpty());
    AttributeReference keyCol =
        new AttributeReference(
            "key_col",
            DataTypes.StringType,
            false,
            Metadata$.MODULE$.empty(),
            exprId2,
            ScalaConversionUtils.asScalaSeqEmpty());
    GetMapValue mapAccess = new GetMapValue(scores, keyCol);

    Alias alias = alias(exprId3, ALIAS_NAME, mapAccess);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    // Map column gets fieldPath = [key_col]
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "scores[key_col] AS res",
                false,
                "[key_col]"));
    // Key column itself is also a TRANSFORMATION dependency
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId2,
            ALIAS_NAME,
            TransformationInfo.transformation("scores[key_col] AS res"));
  }

  @Test
  void testArrayAccessEmitsSentinelFieldPath() {
    // arraycol[5] — actual index value is NOT emitted; sentinel "[0]" is used
    AttributeReference arraycol =
        new AttributeReference(
            "arraycol",
            DataTypes.createArrayType(DataTypes.StringType),
            false,
            Metadata$.MODULE$.empty(),
            exprId1,
            ScalaConversionUtils.asScalaSeqEmpty());
    GetArrayItem arrayAccess =
        new GetArrayItem(arraycol, new Literal(5, IntegerType$.MODULE$), true);

    Alias alias = alias(exprId3, ALIAS_NAME, arrayAccess);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "arraycol[5] AS res",
                false,
                "[0]"));
  }

  @Test
  void testNestedArrayAccessBuildsCumulativeFieldPath() {
    // arraycol[0][1] — both levels collapse to "[0]" sentinel
    AttributeReference arraycol =
        new AttributeReference(
            "arraycol",
            DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)),
            false,
            Metadata$.MODULE$.empty(),
            exprId1,
            ScalaConversionUtils.asScalaSeqEmpty());
    GetArrayItem innerAccess =
        new GetArrayItem(arraycol, new Literal(0, IntegerType$.MODULE$), true);
    GetArrayItem outerAccess =
        new GetArrayItem(innerAccess, new Literal(1, IntegerType$.MODULE$), true);

    Alias alias = alias(exprId3, ALIAS_NAME, outerAccess);
    Project project = new Project(getNamedExpressionSeq(alias), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    // Intermediate: [0]
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "arraycol[0][1] AS res",
                false,
                "[0]"));
    // Full nested path: [0][0]
    verify(builder, times(1))
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "arraycol[0][1] AS res",
                false,
                "[0][0]"));
    // The spurious "[1]"-only entry must NOT appear
    verify(builder, Mockito.never())
        .addDependency(
            exprId3,
            exprId1,
            ALIAS_NAME,
            new TransformationInfo(
                TransformationInfo.Types.DIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "arraycol[0][1] AS res",
                false,
                "[1]"));
  }

  private static Seq<NamedExpression> getNamedExpressionSeq(NamedExpression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.stream(expressions).collect(Collectors.toList()));
  }

  private static Seq<Expression> getExpressionSeq(Expression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.stream(expressions).collect(Collectors.toList()));
  }

  @NotNull
  private AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static AttributeReference structField(String name, StructType type, ExprId exprId) {
    return new AttributeReference(
        name,
        type,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  @NotNull
  private static Alias alias(ExprId aliasExprId, String aliasName, Expression child) {
    return new Alias(
        child,
        aliasName,
        aliasExprId,
        ScalaConversionUtils.asScalaSeqEmpty(),
        Option.empty(),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static void mockNewExprId(LongAccumulator id, MockedStatic<NamedExpression> utilities) {
    utilities
        .when(NamedExpression::newExprId)
        .thenAnswer(
            (Answer<ExprId>)
                invocation -> {
                  ExprId exprId = ExprId.apply(id.get());
                  id.accumulate(1);
                  return exprId;
                });
  }
}
