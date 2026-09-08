/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.fromSeq;
import static java.util.Objects.nonNull;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.ExpressionVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Crc32;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.HiveHash;
import org.apache.spark.sql.catalyst.expressions.Md5;
import org.apache.spark.sql.catalyst.expressions.Murmur3Hash;
import org.apache.spark.sql.catalyst.expressions.Sha1;
import org.apache.spark.sql.catalyst.expressions.Sha2;
import org.apache.spark.sql.catalyst.expressions.XxHash64;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;

@Slf4j
public class ExpressionTraverser {
  // Expressions that obscure source values: the original values cannot be recovered from the
  // output.
  // Hash/digest functions and aggregate functions like Count qualify — COUNT(DISTINCT col) = N
  // tells you nothing about which individual col values were counted.
  private static final List<Class> classes =
      Arrays.asList(
          Crc32.class,
          HiveHash.class,
          Md5.class,
          Murmur3Hash.class,
          Sha1.class,
          Sha2.class,
          XxHash64.class,
          Count.class);

  // Masking expressions not available in all supported versions
  private static final List<String> classNames =
      Collections.singletonList("org.apache.spark.sql.catalyst.expressions.Mask");

  private static Boolean isMasking(Expression expression) {
    return classes.stream().anyMatch(c -> c.equals(expression.getClass()))
        || classNames.stream().anyMatch(n -> n.equals(expression.getClass().getCanonicalName()));
  }

  private final Expression expression;
  private final ExprId outputExpressionId;
  private final String outputExpressionString;
  private final TransformationInfo transformationInfo;
  private final ColumnLevelLineageBuilder builder;
  private final VisitorFactory visitorFactory;

  private ExpressionTraverser(
      Expression expression,
      ExprId outputExpressionId,
      String outputExpressionString,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder,
      VisitorFactory visitorFactory) {
    this.expression = expression;
    this.outputExpressionId = outputExpressionId;
    this.outputExpressionString = outputExpressionString;
    this.transformationInfo = transformationInfo;
    this.builder = builder;
    this.visitorFactory = visitorFactory;
  }

  public static ExpressionTraverser of(
      Expression expression, ExprId outputExpressionId, ColumnLevelLineageBuilder builder) {
    return ExpressionTraverser.of(
        expression,
        outputExpressionId,
        expression.sql(),
        TransformationInfo.identity(expression.sql()),
        builder);
  }

  public static ExpressionTraverser of(
      Expression expression,
      ExprId outputExpressionId,
      String outputExpressionString,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    return new ExpressionTraverser(
        expression,
        outputExpressionId,
        outputExpressionString,
        transformationInfo,
        builder,
        new VisitorFactory());
  }

  public ExpressionTraverser copyFor(Expression expression) {
    return ExpressionTraverser.of(
        expression,
        this.outputExpressionId,
        this.outputExpressionString,
        this.transformationInfo,
        this.builder);
  }

  public ExpressionTraverser copyFor(Expression expression, TransformationInfo transformationInfo) {
    return ExpressionTraverser.of(
        expression,
        this.outputExpressionId,
        this.outputExpressionString,
        this.transformationInfo.merge(transformationInfo),
        this.builder);
  }

  public ExpressionTraverser copyOverrideTransform(
      Expression expression, TransformationInfo transformationInfo) {
    return ExpressionTraverser.of(
        expression,
        this.outputExpressionId,
        this.outputExpressionString,
        transformationInfo,
        this.builder);
  }

  /**
   * Creates a copy for traversal with the accumulated {@code fieldPath} stripped, so it does not
   * leak into the child's base dependency record. Uses merge semantics to preserve INDIRECT type
   * and masking from the outer context. Used in step 1 of field-access visitors.
   */
  public ExpressionTraverser copyForStrippingFieldPath(Expression expression) {
    TransformationInfo merged = this.transformationInfo.merge(TransformationInfo.transformation());
    return copyOverrideTransform(
        expression,
        new TransformationInfo(
            merged.getType(), merged.getSubType(), merged.getDescription(), merged.getMasking()));
  }

  /**
   * Creates a copy for traversal that prepends {@code keySuffix} before the outer {@code
   * fieldPath}, building the full access path bottom-up. Uses merge semantics to preserve INDIRECT
   * type and masking from the outer context. Used in step 2 of field-access visitors.
   *
   * <p>Example: outer fieldPath {@code "[innerkey1]"} + keySuffix {@code "[key1]"} → {@code
   * "[key1][innerkey1]"}.
   */
  public ExpressionTraverser copyWithFieldPath(Expression expression, String keySuffix) {
    String outerPath = this.transformationInfo.getFieldPath();
    String fullPath = outerPath == null ? keySuffix : keySuffix + outerPath;
    TransformationInfo merged = this.transformationInfo.merge(TransformationInfo.transformation());
    return copyOverrideTransform(
        expression,
        new TransformationInfo(
            merged.getType(),
            merged.getSubType(),
            merged.getDescription(),
            merged.getMasking(),
            fullPath));
  }

  public void traverse() {
    if (expression instanceof AttributeReference) {
      AttributeReference attRef = (AttributeReference) expression;
      if (!attRef.exprId().equals(outputExpressionId)) {
        addDependency(attRef.exprId());
      }
      return;
    }
    for (ExpressionVisitor v : visitorFactory.expressionVisitors()) {
      if (v.isDefinedAt(expression)) {
        v.apply(expression, this);
        return;
      }
    }
    if (shouldFallbackToGenericHandling()) {
      fromSeq(expression.children())
          .forEach(
              child ->
                  copyFor(child, TransformationInfo.transformation(isMasking(expression)))
                      .traverse());
    }
  }

  public void addDependency(ExprId inputExprId) {
    builder.addDependency(
        outputExpressionId,
        inputExprId,
        outputExpressionString,
        transformationInfo.merge(TransformationInfo.identity()));
  }

  public void addDependency(ExprId inputExprId, TransformationInfo transformationInfo) {
    builder.addDependency(
        outputExpressionId,
        inputExprId,
        outputExpressionString,
        this.transformationInfo.merge(transformationInfo));
  }

  private boolean shouldFallbackToGenericHandling() {
    return nonNull(expression) && nonNull(expression.children());
  }
}
