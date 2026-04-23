/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.Optional;
import java.util.concurrent.Callable;

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
   * Atomically reads and clears the most recent trip state captured by this breaker, if any. A
   * "trip" is an observed transition from open to closed. Implementations that track transitions
   * should overwrite the captured state on each new trip so the caller sees the latest reason; the
   * reference is cleared by this method so a repeated call (or a repeated {@code close()} on the
   * client) will not re-emit the same trip.
   */
  default Optional<CircuitBreakerState> consumeLastTrip() {
    return Optional.empty();
  }
}
