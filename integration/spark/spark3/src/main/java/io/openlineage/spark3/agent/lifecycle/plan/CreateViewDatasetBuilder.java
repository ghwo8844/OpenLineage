/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches a
 * org.apache.spark.sql.catalyst.plans.logical.CreateV2View and extracts the output {@link
 * OpenLineage.Dataset} being created as a view. This captures view creation with proper schema
 * information and lifecycle state tracking. Column lineage is handled separately at the run event
 * builder level.
 */
@Slf4j
public class CreateViewDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CreateViewDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x.getClass()
        .getName()
        .equals("org.apache.spark.sql.catalyst.plans.logical.CreateV2View");
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    try {
      LifecycleStateChange lifecycleState = LifecycleStateChange.CREATE;
      Object replace = plan.getClass().getMethod("replace").invoke(plan);
      if (replace != null && replace instanceof Boolean && (Boolean) replace) {
        lifecycleState = LifecycleStateChange.OVERWRITE;
      }

      DatasetIdentifier datasetIdentifier = resolveDatasetIdentifier(plan);

      OpenLineage openLineage = context.getOpenLineage();
      DatasetCompositeFacetsBuilder builder = new DatasetCompositeFacetsBuilder(openLineage);

      StructType viewSchema = extractViewSchema(plan);
      if (viewSchema != null) {
        builder.getFacets().schema(PlanUtils.schemaFacet(openLineage, viewSchema));
      }

      builder
          .getFacets()
          .lifecycleStateChange(
              openLineage.newLifecycleStateChangeDatasetFacet(lifecycleState, null))
          .dataSource(PlanUtils.datasourceFacet(openLineage, datasetIdentifier.getNamespace()));

      return Collections.singletonList(
          outputDataset().sparkDatasetBuilder(builder).dataset(datasetIdentifier).build());

    } catch (Exception e) {
      log.warn("Failed to build dataset for CreateView command: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  private DatasetIdentifier resolveDatasetIdentifier(LogicalPlan plan) throws Exception {
    // CreateV2View has catalog: ViewCatalog and ident: Identifier directly
    Object catalogObj = plan.getClass().getMethod("catalog").invoke(plan);
    String catalogName = (String) catalogObj.getClass().getMethod("name").invoke(catalogObj);
    Identifier identifier = (Identifier) plan.getClass().getMethod("ident").invoke(plan);

    return new DatasetIdentifier(buildQualifiedName(catalogName, identifier), "View");
  }

  private String buildQualifiedName(String catalogName, Identifier identifier) {
    String[] ns = identifier.namespace();
    String name = identifier.name().replaceAll("^`+|`+$", "");

    List<String> parts = new ArrayList<>();
    if (ns.length == 0 || !ns[0].equals(catalogName)) {
      parts.add(catalogName);
    }
    Collections.addAll(parts, ns);
    parts.add(name);
    return String.join(".", parts);
  }

  private StructType extractViewSchema(LogicalPlan plan) {
    try {
      Object viewSchema = plan.getClass().getMethod("viewSchema").invoke(plan);
      if (viewSchema != null && viewSchema instanceof StructType) {
        return (StructType) viewSchema;
      }
      return null;
    } catch (Exception e) {
      log.warn("Could not extract schema from CreateView command", e);
      return null;
    }
  }
}
