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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.Test;

public class InitialLimitedDurationErrorInjectionPolicyTest {

  private ObjectNode createInputObject(String duration) {
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    if (duration != null) {
      node.put("duration", duration);
    } else {
      node.set("duration", JsonNodeFactory.instance.nullNode());
    }
    return node;
  }

  @Test
  public void constructor_shouldParseValidDurationFromObject() {
    JsonNode input = createInputObject("PT5S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be 5 seconds",
        Duration.ofSeconds(5),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be stored", "PT5S", policy.getEffectiveDurationParameter());
    assertNull("Start time should be null initially", policy.getStartTime());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfInputMissing() {
    JsonNode input = JsonNodeFactory.instance.missingNode();
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default 10 minutes",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfInputIsNull() {
    JsonNode input = JsonNodeFactory.instance.nullNode();
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default 10 minutes",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfInputIsNotObject() {
    JsonNode input = JsonNodeFactory.instance.textNode("PT5S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default for non-object",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfDurationFieldMissing() {
    ObjectNode input = JsonNodeFactory.instance.objectNode();
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default if field missing",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfDurationFieldNotText() {
    ObjectNode input = JsonNodeFactory.instance.objectNode();
    input.put("duration", 123);
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default if field not text",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test
  public void constructor_shouldUseDefaultDurationIfDurationFieldIsBlank() {
    JsonNode input = createInputObject("  ");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    assertEquals(
        "Parsed duration should be default for blank field",
        Duration.ofMinutes(10),
        policy.getInjectionDuration());
    assertEquals(
        "Effective parameter should be default", "PT10M", policy.getEffectiveDurationParameter());
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructor_shouldThrowExceptionForInvalidDurationFormat() {
    JsonNode input = createInputObject("Invalid Duration");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    new InitialLimitedDurationErrorInjectionPolicy(input, clock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructor_shouldThrowExceptionForZeroDuration() {
    JsonNode input = createInputObject("PT0S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    new InitialLimitedDurationErrorInjectionPolicy(input, clock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructor_shouldThrowExceptionForNegativeDuration() {
    JsonNode input = createInputObject("PT-5S");
    Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    new InitialLimitedDurationErrorInjectionPolicy(input, clock);
  }

  @Test
  public void shouldInjectError_firstCallReturnsTrueAndInitializesTime() {
    JsonNode input = createInputObject("PT2S");
    Instant initialInstant = Instant.parse("2025-04-23T10:00:00Z");
    Clock initialClock = Clock.fixed(initialInstant, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, initialClock);

    assertNull("Start time should be null before first call", policy.getStartTime());

    // First call
    boolean result = policy.shouldInjectionError();

    assertTrue("First call should return true", result);
    assertNotNull("Start time should be set after first call", policy.getStartTime());
    assertEquals("Start time should match first call time", initialInstant, policy.getStartTime());
  }

  @Test
  public void shouldInjectError_multipleCallsMaintainStateAndRespectDuration() {
    JsonNode input = createInputObject("PT3S"); // 3 seconds duration
    Instant t0 = Instant.parse("2025-04-23T11:00:00Z");
    Clock clock = Clock.fixed(t0, ZoneOffset.UTC);

    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    System.out.println(policy);

    // Simulate calls at different times by changing the clock

    // t=0s (First call)
    assertTrue("Call at t=0s should return true", policy.shouldInjectionError());
    assertEquals("Start time should be t0", t0, policy.getStartTime());

    // t=1s
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(1), ZoneOffset.UTC));
    assertTrue("Call at t=1s should return true", policy.shouldInjectionError());

    // t=2s
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(2), ZoneOffset.UTC));
    assertTrue("Call at t=2s should return true", policy.shouldInjectionError());

    // t=3s
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(3), ZoneOffset.UTC));
    assertFalse("Call at t=3s should return false", policy.shouldInjectionError());

    // t=4s
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(4), ZoneOffset.UTC));
    assertFalse("Call at t=4s should return false", policy.shouldInjectionError());

    // t=10s
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(10), ZoneOffset.UTC));
    assertFalse("Call at t=10s should return false", policy.shouldInjectionError());
  }

  @Test
  public void shouldInjectError_startTimeDoesNotChangeAfterFirstCall() {
    JsonNode input = createInputObject("PT5S");
    Instant t0 = Instant.parse("2025-04-23T12:00:00Z");
    Clock clock = Clock.fixed(t0, ZoneOffset.UTC);
    InitialLimitedDurationErrorInjectionPolicy policy =
        new InitialLimitedDurationErrorInjectionPolicy(input, clock);

    // First call
    policy.shouldInjectionError();
    Instant firstStartTime = policy.getStartTime();
    assertNotNull(firstStartTime);

    // Second call
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(2), ZoneOffset.UTC));
    policy.shouldInjectionError();
    Instant secondStartTime = policy.getStartTime();
    assertEquals("Start time should not change after first call", firstStartTime, secondStartTime);

    // Third call
    policy.setClockForTesting(Clock.fixed(t0.plusSeconds(10), ZoneOffset.UTC));
    policy.shouldInjectionError();
    Instant thirdStartTime = policy.getStartTime();
    assertEquals("Start time should still not change", firstStartTime, thirdStartTime);
  }
}
