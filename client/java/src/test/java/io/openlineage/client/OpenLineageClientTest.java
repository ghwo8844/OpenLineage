/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.circuitBreaker.CircuitBreakerState;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportErrorRunFacet;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OpenLineageClientTest {

  CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
  Transport transport = mock(Transport.class);
  MeterRegistry meterRegistry = new SimpleMeterRegistry();
  OpenLineageClient client = new OpenLineageClient(transport, circuitBreaker, meterRegistry);

  @Test
  void testEmit() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));

    OpenLineage.RunEvent runEvent = mock(RunEvent.class);
    client.emit(runEvent);
    verify(transport, times(1)).emit(runEvent);

    OpenLineage.DatasetEvent datasetEvent = mock(DatasetEvent.class);
    client.emit(datasetEvent);
    verify(transport, times(1)).emit(datasetEvent);

    OpenLineage.JobEvent jobEvent = mock(JobEvent.class);
    client.emit(jobEvent);
    verify(transport, times(1)).emit(jobEvent);
  }

  @SneakyThrows
  @Test
  void testClose() {
    client.close();
    verify(transport, times(1)).close();
    verify(circuitBreaker, times(1)).close();
  }

  @SneakyThrows
  @Test
  void testCloseFails() {
    RuntimeException nestedException = new RuntimeException("Transport failed");
    doThrow(nestedException).when(transport).close();

    OpenLineageClientException exception =
        assertThrows(OpenLineageClientException.class, () -> client.close());
    assertThat(exception.getCause()).isEqualTo(nestedException);

    verify(transport, times(1)).close();
    verify(circuitBreaker, times(1)).close(); // called even if transport fails
  }

  @Test
  void testCircuitBreakerForEmitRunEvent() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(true));
    client.emit(mock(RunEvent.class));
    verify(transport, times(0)).emit(any(RunEvent.class));

    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    client.emit(mock(RunEvent.class));
    verify(transport, times(1)).emit(any(RunEvent.class));
  }

  @Test
  void testCircuitBreakerForEmitJobEvent() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(true));
    client.emit(mock(JobEvent.class));
    verify(transport, times(0)).emit(any(JobEvent.class));

    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    client.emit(mock(JobEvent.class));
    verify(transport, times(1)).emit(any(JobEvent.class));
  }

  @Test
  void testCircuitBreakerForEmitDatasetEvent() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(true));
    client.emit(mock(DatasetEvent.class));
    verify(transport, times(0)).emit(any(DatasetEvent.class));

    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    client.emit(mock(DatasetEvent.class));
    verify(transport, times(1)).emit(any(DatasetEvent.class));
  }

  @Test
  void emitRunEventEmitsTransportErrorSignalOnFailure() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    RuntimeException transportError =
        new RuntimeException("HTTP error 400: PayloadTooLargeException");
    doThrow(transportError).doNothing().when(transport).emit(any(RunEvent.class));

    RunEvent runEvent = mock(RunEvent.class);
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> client.emit(runEvent));
    assertThat(thrown).isSameAs(transportError);

    ArgumentCaptor<RunEvent> captor = ArgumentCaptor.forClass(RunEvent.class);
    verify(transport, times(2)).emit(captor.capture());
    List<RunEvent> events = captor.getAllValues();
    assertThat(events.get(0)).isSameAs(runEvent);

    RunEvent signal = events.get(1);
    assertThat(signal.getJob().getNamespace()).isEqualTo("openlineage");
    assertThat(signal.getJob().getName()).isEqualTo("transport-error");
    assertThat(signal.getRun().getFacets().getAdditionalProperties()).containsKey("transportError");

    TransportErrorRunFacet facet =
        (TransportErrorRunFacet)
            signal.getRun().getFacets().getAdditionalProperties().get("transportError");
    assertThat(facet.getErrorClass()).isEqualTo("java.lang.RuntimeException");
    assertThat(facet.getErrorMessage()).isEqualTo("HTTP error 400: PayloadTooLargeException");
    assertThat(facet.getOriginalEvent()).contains("RunEvent");
  }

  @Test
  void emitDatasetEventEmitsTransportErrorSignalOnFailure() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    RuntimeException transportError = new RuntimeException("dataset transport boom");
    doThrow(transportError).when(transport).emit(any(DatasetEvent.class));
    doNothing().when(transport).emit(any(RunEvent.class));

    DatasetEvent datasetEvent = mock(DatasetEvent.class);
    assertThrows(RuntimeException.class, () -> client.emit(datasetEvent));

    ArgumentCaptor<RunEvent> captor = ArgumentCaptor.forClass(RunEvent.class);
    verify(transport, times(1)).emit(captor.capture());
    RunEvent signal = captor.getValue();
    assertThat(signal.getJob().getName()).isEqualTo("transport-error");
    TransportErrorRunFacet facet =
        (TransportErrorRunFacet)
            signal.getRun().getFacets().getAdditionalProperties().get("transportError");
    assertThat(facet.getOriginalEvent()).contains("DatasetEvent");
  }

  @Test
  void emitJobEventEmitsTransportErrorSignalOnFailure() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    RuntimeException transportError = new RuntimeException("job transport boom");
    doThrow(transportError).when(transport).emit(any(JobEvent.class));
    doNothing().when(transport).emit(any(RunEvent.class));

    JobEvent jobEvent = mock(JobEvent.class);
    assertThrows(RuntimeException.class, () -> client.emit(jobEvent));

    ArgumentCaptor<RunEvent> captor = ArgumentCaptor.forClass(RunEvent.class);
    verify(transport, times(1)).emit(captor.capture());
    TransportErrorRunFacet facet =
        (TransportErrorRunFacet)
            captor.getValue().getRun().getFacets().getAdditionalProperties().get("transportError");
    assertThat(facet.getOriginalEvent()).contains("JobEvent");
  }

  @Test
  void transportErrorSignalFailureDoesNotPropagate() {
    when(circuitBreaker.currentState()).thenReturn(new CircuitBreakerState(false));
    RuntimeException transportError = new RuntimeException("transport fully broken");
    doThrow(transportError).when(transport).emit(any(RunEvent.class));

    RunEvent runEvent = mock(RunEvent.class);
    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> client.emit(runEvent));
    assertThat(thrown).isSameAs(transportError);

    // Two attempts: original event + signal event; signal failure was swallowed.
    verify(transport, times(2)).emit(any(RunEvent.class));
  }
}
