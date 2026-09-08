/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class CommonCircuitBreakerTest {
  Callable<Object> callableWithException =
      (() -> {
        throw new RuntimeException("This should not happen with closed circuit breaker");
      });

  @Test
  void testCallableNotRunWhenCircuitBreakerClosed() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true")).build();
    assertThat(circuitBreaker.run(callableWithException)).isNull();
  }

  @Test
  void verifyNoExecutorServiceIsRunForNoOpCircuitBreaker() {
    CircuitBreaker circuitBreaker = new CircuitBreakerFactory(null).build();
    Callable<Integer> callable = (() -> 1);
    assertThat(circuitBreaker.run(callable)).isEqualTo(1);
  }

  @Test
  void verifyExceptionInCallableIsSwallowed() {
    CircuitBreaker circuitBreaker = new CircuitBreakerFactory(null).build();
    assertThat(circuitBreaker.run(callableWithException)).isNull();
  }

  @Test
  void verifyCircuitBreakerInterruptsCallable() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,true", 50)).build();
    Callable<Object> longLastingCallable =
        (() -> {
          Thread.sleep(2000);
          return null;
        });

    long millisBefore = System.currentTimeMillis();
    circuitBreaker.run(longLastingCallable);
    long millisAfter = System.currentTimeMillis();

    assertThat(millisAfter - millisBefore).isGreaterThanOrEqualTo(50); // proves callable started
    assertThat(millisAfter - millisBefore).isLessThan(2000); // proves callable has been interrupted
  }

  @Test
  void verifyCallableSucceeds() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,false,false", 50))
            .build();
    Callable<Integer> longLastingCallable =
        (() -> {
          Thread.sleep(100);
          return 1;
        });

    assertThat(circuitBreaker.run(longLastingCallable)).isEqualTo(1);
  }

  @Test
  void onTripListenerFiresOnceWhenClosedAtEntry() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true")).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    circuitBreaker.run(callableWithException);

    assertThat(trips).hasSize(1);
    assertThat(trips.get(0).isClosed()).isTrue();
  }

  @Test
  void onTripListenerFiresOnceWhenClosedOnFirstBackgroundPoll() {
    // Sequence: entry check (false) → background first poll (true).
    // Loop body never runs; the post-loop branch fires with the trip,
    // and observe() catches the open→closed transition exactly once.
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,true", 50)).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    Callable<Object> longLastingCallable =
        (() -> {
          Thread.sleep(2000);
          return null;
        });
    circuitBreaker.run(longLastingCallable);

    assertThat(trips).hasSize(1);
    assertThat(trips.get(0).isClosed()).isTrue();
  }

  @Test
  void onTripListenerFiresOnceWhenClosedMidLoop() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,true", 50)).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    Callable<Object> longLastingCallable =
        (() -> {
          Thread.sleep(2000);
          return null;
        });
    circuitBreaker.run(longLastingCallable);

    assertThat(trips).hasSize(1);
    assertThat(trips.get(0).isClosed()).isTrue();
  }

  @Test
  void onTripListenerDedupesAcrossRunsWhileStillClosed() {
    // Multiple run() calls while the breaker stays closed: listener still fires only once.
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true,true,true,true")).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    circuitBreaker.run(callableWithException);
    circuitBreaker.run(callableWithException);
    circuitBreaker.run(callableWithException);

    assertThat(trips).hasSize(1);
  }

  @Test
  void onTripListenerFiresAgainAfterReopen() {
    // false → true (transition #1) → false (reset) → true (transition #2)
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true,false,true")).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    circuitBreaker.run(callableWithException); // entry: closed → trip #1
    circuitBreaker.run(() -> 1); // entry: open → reset
    circuitBreaker.run(callableWithException); // entry: closed → trip #2

    assertThat(trips).hasSize(2);
  }

  @Test
  void onTripListenerNeverFiresWhenBreakerStaysOpen() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,false,false", 50))
            .build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    circuitBreaker.run(() -> 1);

    assertThat(trips).isEmpty();
  }

  @Test
  void onTripListenerExceptionsAreSwallowed() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true")).build();
    circuitBreaker.setOnTripListener(
        s -> {
          throw new RuntimeException("listener boom");
        });

    // Should not throw; run() still returns null because the breaker is closed at entry.
    assertThat(circuitBreaker.run(callableWithException)).isNull();
  }

  @Test
  void noOpCircuitBreakerListenerNeverFires() {
    CircuitBreaker circuitBreaker = new CircuitBreakerFactory(null).build();
    List<CircuitBreakerState> trips = new ArrayList<>();
    circuitBreaker.setOnTripListener(trips::add);

    circuitBreaker.run(() -> 1);

    assertThat(trips).isEmpty();
  }
}
