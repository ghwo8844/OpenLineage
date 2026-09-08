/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportErrorRunFacet;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** HTTP client used to emit {@link OpenLineage.RunEvent}s to HTTP backend. */
@Slf4j
public final class OpenLineageClient implements AutoCloseable {
  final Transport transport;
  final Optional<CircuitBreaker> circuitBreaker;
  final MeterRegistry meterRegistry;
  final String[] disabledFacets;

  Counter emitStart;
  Counter emitComplete;
  AtomicInteger engagedCircuitBreaker;
  Timer emitTime;

  /** Creates a new {@code OpenLineageClient} object. */
  public OpenLineageClient() {
    this(new ConsoleTransport());
  }

  public OpenLineageClient(@NonNull final Transport transport) {
    this(transport, new String[] {});
  }

  public OpenLineageClient(@NonNull final Transport transport, String... disabledFacets) {
    this(transport, null, null, disabledFacets);
  }

  public OpenLineageClient(
      @NonNull final Transport transport,
      CircuitBreaker circuitBreaker,
      MeterRegistry meterRegistry,
      String... disabledFacets) {
    this.transport = transport;
    this.disabledFacets = Arrays.copyOf(disabledFacets, disabledFacets.length);
    this.circuitBreaker = Optional.ofNullable(circuitBreaker);
    if (meterRegistry == null) {
      this.meterRegistry = MicrometerProvider.getMeterRegistry();
    } else {
      this.meterRegistry = meterRegistry;
    }

    initializeMetrics();
    OpenLineageClientUtils.configureObjectMapper(disabledFacets);
  }

  /**
   * Emit the given run event to HTTP backend. The method will return successfully after the run
   * event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param runEvent The run event to emit.
   */
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emitRunEvent(runEvent, () -> transport.emit(runEvent));
  }

  /**
   * Emit the given run event with a bounded wait when supported by the configured transport.
   *
   * <p>Transports that do not support bounded delivery acknowledgement may fall back to regular
   * asynchronous emit semantics.
   *
   * @param runEvent The run event to emit.
   * @param timeout Maximum time to wait for transports that support bounded delivery.
   */
  public void emit(@NonNull OpenLineage.RunEvent runEvent, @NonNull Duration timeout) {
    emitRunEvent(runEvent, () -> transport.emit(runEvent, timeout));
  }

  private void emitRunEvent(@NonNull OpenLineage.RunEvent runEvent, Runnable emit) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}", OpenLineageClientUtils.toJson(runEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    emitStart.increment();
    try {
      emitTime.record(emit);
    } catch (RuntimeException e) {
      emitTransportErrorSignal(summarizeSafe(runEvent), e);
      throw e;
    }
    emitComplete.increment();
  }

  /**
   * Emit the given dataset event to HTTP backend. The method will return successfully after the
   * dataset event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param datasetEvent The dataset event to emit.
   */
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}",
          OpenLineageClientUtils.toJson(datasetEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    emitStart.increment();
    try {
      emitTime.record(() -> transport.emit(datasetEvent));
    } catch (RuntimeException e) {
      emitTransportErrorSignal(summarizeSafe(datasetEvent), e);
      throw e;
    }
    emitComplete.increment();
  }

  /**
   * Emit the given run event to HTTP backend. The method will return successfully after the run
   * event has been emitted, regardless of any exceptions thrown by the HTTP backend.
   *
   * @param jobEvent The job event to emit.
   */
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    if (log.isDebugEnabled()) {
      log.debug(
          "OpenLineageClient will emit lineage event: {}", OpenLineageClientUtils.toJson(jobEvent));
    }
    if (circuitBreaker.isPresent() && circuitBreaker.get().currentState().isClosed()) {
      engagedCircuitBreaker.set(1);
      log.warn("OpenLineageClient disabled with circuit breaker");
      return;
    } else {
      engagedCircuitBreaker.set(0);
    }
    emitStart.increment();
    try {
      emitTime.record(() -> transport.emit(jobEvent));
    } catch (RuntimeException e) {
      emitTransportErrorSignal(summarizeSafe(jobEvent), e);
      throw e;
    }
    emitComplete.increment();
  }

  public void initializeMetrics() {
    emitStart =
        this.meterRegistry.counter(
            "openlineage.emit.start", "openlineage.transport", transport.getClass().getName());
    emitComplete =
        this.meterRegistry.counter(
            "openlineage.emit.complete", "openlineage.transport", transport.getClass().getName());
    engagedCircuitBreaker =
        this.meterRegistry.gauge(
            "openlineage.circuitbreaker.engaged",
            Collections.singletonList(
                Tag.of(
                    "openlineage.circuitbreaker",
                    circuitBreaker.map(cb -> cb.getClass().getSimpleName()).orElse("none"))),
            new AtomicInteger(0));
    emitTime =
        this.meterRegistry.timer(
            "openlineage.emit.time", "openlineage.transport", transport.getClass().getName());
  }

  /** Shutdown the underlying transport, waiting for all events to complete. */
  @Override
  public void close() throws Exception {
    try {
      transport.close();
    } catch (Exception e) {
      throw new OpenLineageClientException("Failed to close transport " + transport, e);
    } finally {
      circuitBreaker.ifPresent(CircuitBreaker::close);
      meterRegistry.close();
      OpenLineageClientUtils.getExecutor().ifPresent(ExecutorService::shutdown);
    }
  }

  private void emitTransportErrorSignal(String originalEventSummary, Throwable error) {
    try {
      transport.emit(buildTransportErrorSignal(originalEventSummary, error));
    } catch (Exception signalFailure) {
      log.warn("Failed to emit transport error signal for {}", originalEventSummary, signalFailure);
    }
  }

  private static OpenLineage.RunEvent buildTransportErrorSignal(
      String originalEventSummary, Throwable error) {
    OpenLineage ol = new OpenLineage(TransportErrorRunFacet.PRODUCER_URI);
    OpenLineage.RunFacets runFacets =
        ol.newRunFacetsBuilder()
            .put(
                "transportError",
                new TransportErrorRunFacet(
                    error.getClass().getName(),
                    truncate(error.getMessage(), MAX_ERROR_MESSAGE_LENGTH),
                    originalEventSummary))
            .build();
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
        .eventType(OpenLineage.RunEvent.EventType.OTHER)
        .run(ol.newRun(UUID.randomUUID(), runFacets))
        .job(ol.newJob("openlineage", "transport-error", ol.newJobFacetsBuilder().build()))
        .build();
  }

  private static final String TRUNCATION_SUFFIX = "...[truncated]";
  private static final int MAX_ERROR_MESSAGE_LENGTH = 4096;

  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return null;
    }
    if (s.length() <= maxLen) {
      return s;
    }
    int cutAt = maxLen - TRUNCATION_SUFFIX.length();
    return cutAt > 0 ? s.substring(0, cutAt) + TRUNCATION_SUFFIX : s.substring(0, maxLen);
  }

  // Never throws — wraps summarize in try/catch so the original transport exception is never masked
  private static String summarizeSafe(OpenLineage.RunEvent e) {
    try {
      return String.format(
          "RunEvent[runId=%s, job=%s/%s, eventType=%s]",
          e.getRun() != null ? e.getRun().getRunId() : null,
          e.getJob() != null ? e.getJob().getNamespace() : null,
          e.getJob() != null ? e.getJob().getName() : null,
          e.getEventType());
    } catch (Exception ignored) {
      return "RunEvent[unparseable]";
    }
  }

  private static String summarizeSafe(OpenLineage.DatasetEvent e) {
    try {
      return String.format(
          "DatasetEvent[dataset=%s/%s]",
          e.getDataset() != null ? e.getDataset().getNamespace() : null,
          e.getDataset() != null ? e.getDataset().getName() : null);
    } catch (Exception ignored) {
      return "DatasetEvent[unparseable]";
    }
  }

  private static String summarizeSafe(OpenLineage.JobEvent e) {
    try {
      return String.format(
          "JobEvent[job=%s/%s]",
          e.getJob() != null ? e.getJob().getNamespace() : null,
          e.getJob() != null ? e.getJob().getName() : null);
    } catch (Exception ignored) {
      return "JobEvent[unparseable]";
    }
  }

  /**
   * @return an new {@link OpenLineageClient.Builder} object for building {@link
   *     OpenLineageClient}s.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link OpenLineageClient} instances.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * OpenLineageClient client = OpenLineageClient().builder()
   *     .url("http://localhost:5000")
   *     .build()
   * }</pre>
   */
  public static final class Builder {
    private static final Transport DEFAULT_TRANSPORT = new ConsoleTransport();
    private Transport transport;
    private String[] disabledFacets;
    private CircuitBreaker circuitBreaker;
    private MeterRegistry meterRegistry;

    private Builder() {
      this.transport = DEFAULT_TRANSPORT;
      disabledFacets = new String[] {};
    }

    public Builder transport(@NonNull Transport transport) {
      this.transport = transport;
      return this;
    }

    public Builder circuitBreaker(@NonNull CircuitBreaker circuitBreaker) {
      this.circuitBreaker = circuitBreaker;
      return this;
    }

    public Builder meterRegistry(@NonNull MeterRegistry meterRegistry) {
      this.meterRegistry = meterRegistry;
      return this;
    }

    public Builder disableFacets(@NonNull String... disabledFacets) {
      this.disabledFacets = Arrays.copyOf(disabledFacets, disabledFacets.length);
      return this;
    }

    /**
     * @return an {@link OpenLineageClient} object with the properties of this {@link
     *     OpenLineageClient.Builder}.
     */
    public OpenLineageClient build() {
      return new OpenLineageClient(transport, circuitBreaker, meterRegistry, disabledFacets);
    }
  }
}
