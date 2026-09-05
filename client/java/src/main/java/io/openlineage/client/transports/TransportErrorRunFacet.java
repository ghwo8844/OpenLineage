/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import java.net.URI;

public class TransportErrorRunFacet extends OpenLineage.DefaultRunFacet {

  public static final URI PRODUCER_URI =
      URI.create("https://github.com/OpenLineage/OpenLineage/tree/main/client/java");

  @JsonProperty("errorClass")
  private final String errorClass;

  @JsonProperty("errorMessage")
  private final String errorMessage;

  @JsonProperty("originalEvent")
  private final String originalEvent;

  public TransportErrorRunFacet(String errorClass, String errorMessage, String originalEvent) {
    super(PRODUCER_URI);
    this.errorClass = errorClass;
    this.errorMessage = errorMessage;
    this.originalEvent = originalEvent;
  }

  public String getErrorClass() {
    return errorClass;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getOriginalEvent() {
    return originalEvent;
  }
}
