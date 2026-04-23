/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import java.net.URI;

/**
 * Run facet emitted alongside a synthetic circuit-breaker signal event when the
 * OpenLineage circuit breaker has tripped during a client's lifetime. The signal
 * is emitted from {@code OpenLineageClient.close()} so lineage backends can
 * surface trips in-band with normal lineage traffic.
 */
public class CircuitBreakerRunFacet extends OpenLineage.DefaultRunFacet {

  public static final URI PRODUCER_URI =
      URI.create("https://github.com/OpenLineage/OpenLineage/tree/main/client/java");

  @JsonProperty("type")
  private final String type;

  @JsonProperty("reason")
  private final String reason;

  public CircuitBreakerRunFacet(String type, String reason) {
    super(PRODUCER_URI);
    this.type = type;
    this.reason = reason;
  }

  public String getType() {
    return type;
  }

  public String getReason() {
    return reason;
  }
}
