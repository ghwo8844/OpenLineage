/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public interface CircuitBreaker {

  int CIRCUIT_CHECK_INTERVAL_IN_MILLIS = 1000;

  CircuitBreakerState currentState();

  /**
   * @param callable The callable to be run
   * @return result of callable
   * @param <T> callable generic type
   */
  <T> T run(Callable<T> callable);

  void close();

  default int getCheckIntervalMillis() {
    return CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  }

  /**
   * Register a listener that is invoked exactly once per open→closed transition (the moment the
   * breaker trips), with the immutable {@link CircuitBreakerState} captured at that moment. The
   * listener is called synchronously on whatever thread observed the transition; implementations
   * are expected to swallow listener exceptions so they cannot break the breaker.
   */
  default void setOnTripListener(Consumer<CircuitBreakerState> listener) {}
}
