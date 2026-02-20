/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import lombok.Getter;
import lombok.NonNull;

/** Captures information related to Apache Spark SQL execution. */
@Getter
public class SparkSQLExecutionDetailsFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("executionId")
  @NonNull
  private Long executionId;

  @JsonProperty("executionName")
  private String executionName;

  public SparkSQLExecutionDetailsFacet(@NonNull Long executionId, String executionName) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.executionId = executionId;
    this.executionName = executionName;
  }
}
