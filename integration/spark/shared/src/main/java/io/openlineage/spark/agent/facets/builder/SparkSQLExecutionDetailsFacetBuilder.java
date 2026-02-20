/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkSQLExecutionDetailsFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.function.BiConsumer;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import scala.Option;

/**
 * {@link CustomFacetBuilder} that adds the {@link SparkSQLExecutionDetailsFacet} to a run. This
 * facet is generated for every {@link SparkListenerSQLExecutionEnd}.
 */
public class SparkSQLExecutionDetailsFacetBuilder
    extends CustomFacetBuilder<SparkListenerSQLExecutionEnd, SparkSQLExecutionDetailsFacet> {

  @Override
  protected void build(
      SparkListenerSQLExecutionEnd event,
      BiConsumer<String, ? super SparkSQLExecutionDetailsFacet> consumer) {
    consumer.accept(
        "spark_sqlExecutionDetails",
        new SparkSQLExecutionDetailsFacet(event.executionId(), extractExecutionName(event)));
  }

  /** Extracts executionName from the event. Returns null when executionName is not defined. */
  private String extractExecutionName(SparkListenerSQLExecutionEnd event) {
    Option<String> name = event.executionName();
    return name.isDefined() ? name.get() : null;
  }
}
