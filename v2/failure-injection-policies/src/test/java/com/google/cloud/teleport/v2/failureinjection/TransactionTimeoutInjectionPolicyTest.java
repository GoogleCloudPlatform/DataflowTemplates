/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.failureinjection;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import org.junit.Test;

/** Test class for {@link TransactionTimeoutInjectionPolicy}. */
public class TransactionTimeoutInjectionPolicyTest {

  private ObjectNode createInputObject(String windowDuration, String delayDuration) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    if (windowDuration != null) {
      node.put("transactionTimeoutBakeDuration", windowDuration);
    }
    if (delayDuration != null) {
      node.put("transactionDelayDuration", delayDuration);
    }
    return node;
  }

  @Test
  public void constructor_shouldParseValidDurations() {
    JsonNode input = createInputObject("PT5S", "PT0.5S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    assertEquals(Duration.ofMillis(500), policy.getDelay());
  }

  @Test
  public void constructor_shouldUseDefaultsWhenInputMissing() {
    JsonNode input = JsonNodeFactory.instance.missingNode();
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    assertEquals(Duration.ofSeconds(260), policy.getDelay());
  }

  @Test
  public void constructor_shouldUseDefaultsWhenInputNull() {
    JsonNode input = JsonNodeFactory.instance.nullNode();
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    assertEquals(Duration.ofSeconds(260), policy.getDelay());
  }

  @Test
  public void constructor_shouldUseDefaultsWhenFieldsMissing() {
    ObjectNode input = JsonNodeFactory.instance.objectNode(); // Empty object
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    assertEquals(Duration.ofSeconds(260), policy.getDelay());
  }

  @Test
  public void constructor_shouldThrowExceptionForInvalidWindowDuration() {
    JsonNode input = createInputObject("Invalid", "PT1S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new TransactionTimeoutInjectionPolicy(input, clock));
    assertThat(e).hasMessageThat().contains("Failed to parse transactionTimeoutBakeDuration");
  }

  @Test
  public void constructor_shouldThrowExceptionForInvalidDelayDuration() {
    JsonNode input = createInputObject("PT1M", "Invalid");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new TransactionTimeoutInjectionPolicy(input, clock));
    assertThat(e).hasMessageThat().contains("Failed to parse transactionDelayDuration");
  }

  @Test
  public void shouldInjectDelay_respectsTimeWindow() {
    JsonNode input = createInputObject("PT5S", "PT0.1S"); // 5-second window
    Instant t0 = Instant.parse("2025-01-01T00:00:00Z");
    Clock clock = Clock.fixed(t0, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    // Force delay injection
    policy.setRandomForTesting(
        new Random() {
          @Override
          public double nextDouble() {
            return 0.1; // Less than 0.2
          }
        });

    // First call at t=0s, should be inside the window
    long startTime = System.currentTimeMillis();
    policy.shouldInjectionError();
    long endTime = System.currentTimeMillis();
    assertThat(endTime - startTime).isAtLeast(100L); // Check that delay was applied

    // Move clock forward by 4 seconds (t=4s), still inside the window
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(4), ZoneOffset.UTC));
    startTime = System.currentTimeMillis();
    policy.shouldInjectionError();
    endTime = System.currentTimeMillis();
    assertThat(endTime - startTime).isAtLeast(100L);

    // Move clock forward by 6 seconds (t=6s), now outside the window
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(6), ZoneOffset.UTC));
    startTime = System.currentTimeMillis();
    policy.shouldInjectionError();
    endTime = System.currentTimeMillis();
    assertThat(endTime - startTime).isLessThan(50L); // No significant delay
  }

  @Test
  public void shouldInjectDelay_respectsProbability() {
    JsonNode input = createInputObject("PT5S", "PT0.1S");
    Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    // Case 1: Random < 0.2 -> Should delay
    policy.setRandomForTesting(
        new Random() {
          @Override
          public double nextDouble() {
            return 0.1;
          }
        });
    long startTime = System.currentTimeMillis();
    policy.shouldInjectionError();
    long endTime = System.currentTimeMillis();
    assertThat(endTime - startTime).isAtLeast(100L);

    // Case 2: Random >= 0.2 -> Should NOT delay
    policy.setRandomForTesting(
        new Random() {
          @Override
          public double nextDouble() {
            return 0.3;
          }
        });
    startTime = System.currentTimeMillis();
    policy.shouldInjectionError();
    endTime = System.currentTimeMillis();
    assertThat(endTime - startTime).isLessThan(50L);
  }

  @Test
  public void shouldInjectDelay_isThreadSafe() throws InterruptedException {
    JsonNode input = createInputObject("PT0.5S", "PT0.05S"); // 0.5-second window, 50ms delay
    Instant t0 = Instant.parse("2025-01-01T00:00:00Z");
    Clock clock = Clock.fixed(t0, ZoneOffset.UTC);
    TransactionTimeoutInjectionPolicy policy = new TransactionTimeoutInjectionPolicy(input, clock);

    // Force delay injection for all threads
    policy.setRandomForTesting(
        new Random() {
          @Override
          public double nextDouble() {
            return 0.1;
          }
        });

    // All threads should experience a delay during the initial window.
    // The test will take ~2 seconds to run.
    // All threads should experience a delay during this window.
    long testStart = System.currentTimeMillis();
    Runnable task =
        () -> {
          long taskStart = System.currentTimeMillis();
          policy.shouldInjectionError();
          long taskEnd = System.currentTimeMillis();
          assertThat(taskEnd - taskStart).isAtLeast(40L);
        };

    Thread t1 = new Thread(task);
    Thread t2 = new Thread(task);

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Move clock forward past the injection window.
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(1), ZoneOffset.UTC));

    long finalTaskStart = System.currentTimeMillis();
    policy.shouldInjectionError();
    long finalTaskEnd = System.currentTimeMillis();
    assertThat(finalTaskEnd - finalTaskStart).isLessThan(50L);
  }

  @Test
  public void shouldInjectError_alwaysReturnsFalse() {
    JsonNode input = createInputObject("PT1S", "PT0.1S");
    TransactionTimeoutInjectionPolicy policy =
        new TransactionTimeoutInjectionPolicy(input, Clock.systemUTC());

    // The method should always return false, as its purpose is to delay, not to inject an error.
    assertThat(policy.shouldInjectionError()).isFalse();
  }
}
