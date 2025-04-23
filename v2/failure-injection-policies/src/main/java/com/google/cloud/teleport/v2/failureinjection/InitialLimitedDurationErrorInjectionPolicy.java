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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ErrorInjectionPolicy} that injects errors only for a specified duration starting from
 * the moment the shouldInjectError method is called for the first time. After the duration has
 * passed, it will no longer signal for errors to be injected.
 */
public class InitialLimitedDurationErrorInjectionPolicy
    implements ErrorInjectionPolicy, Serializable {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialLimitedDurationErrorInjectionPolicy.class);
  private static final long serialVersionUID = 1L;

  private Instant startTime;
  private final Duration injectionDuration;
  private final String effectiveDurationParameter;
  private Clock clock;

  private static final String DEFAULT_DURATION = "PT10M";
  private static final String DURATION_FIELD_IN_OBJECT = "duration";

  public InitialLimitedDurationErrorInjectionPolicy(JsonNode inputParameter) {
    this(inputParameter, Clock.systemUTC());
  }

  /**
   * Creates a new policy that injects errors for a limited initial duration, configured via a
   * JsonNode.
   *
   * <p>The inputParameter can be: 1. An ObjectNode containing a field named "duration" with the
   * ISO-8601 string (e.g., {"duration": "PT5M"}). 2. MissingNode, NullNode, blank text, or other
   * types will result in using the default duration. ("PT10M").durationParameter
   *
   * @param inputParameter The JsonNode containing the configuration.
   * @param clock The clock source to use for time operations.
   * @throws IllegalArgumentException if the resolved duration string is unparseable or results in a
   *     non-positive duration.
   */
  public InitialLimitedDurationErrorInjectionPolicy(JsonNode inputParameter, Clock clock) {
    this.clock = clock;
    String durationString = DEFAULT_DURATION;

    if (inputParameter != null && !inputParameter.isMissingNode() && !inputParameter.isNull()) {
      if (inputParameter.isObject()) {
        JsonNode durationPath = inputParameter.path(DURATION_FIELD_IN_OBJECT);
        if (durationPath.isTextual()) {
          String textValue = durationPath.asText();
          if (textValue != null && !textValue.isBlank()) {
            durationString = textValue;
          } else {
            LOG.warn(
                "Received object input with blank text in field '{}'. Using default: {}",
                DURATION_FIELD_IN_OBJECT,
                DEFAULT_DURATION);
          }
        }
      }
    }

    this.effectiveDurationParameter = durationString;

    // Parse and validate the determined duration string
    try {
      this.injectionDuration = Duration.parse(this.effectiveDurationParameter);
      if (this.injectionDuration.isZero() || this.injectionDuration.isNegative()) {
        throw new IllegalArgumentException(
            "Injection duration must be positive. Effective duration: "
                + this.effectiveDurationParameter);
      }
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Failed to parse duration parameter. Expected ISO-8601 format (e.g., 'PT10M'). Effective duration: "
              + this.effectiveDurationParameter,
          e);
    }
  }

  /**
   * Determines whether an error should be injected based on the elapsed time since instantiation.
   *
   * @return {@code true} for the {@code injectionDuration}, later returns {@code false}.
   */
  @Override
  public boolean shouldInjectionError() {
    if (this.startTime == null) {
      synchronized (this) {
        if (this.startTime == null) {
          this.startTime = Instant.now(clock);
          LOG.info(
              "First call detected. Errors will be injected for {} starting from {}.",
              this.injectionDuration,
              this.startTime);
        }
      }
    }

    Instant now = Instant.now(clock);
    Duration elapsed = Duration.between(startTime, now);

    // Compare elapsed time with the configured duration.
    // elapsed.compareTo(injectionDuration) < 0 means elapsed < injectionDuration
    boolean shouldInject = elapsed.compareTo(injectionDuration) < 0;

    if (shouldInject) {
      LOG.trace(
          "Injecting error: Elapsed time {} is within the allowed duration {}.",
          elapsed,
          injectionDuration);
    } else {
      LOG.trace(
          "Stopping error injection: Elapsed time {} has exceeded the allowed duration {}.",
          elapsed,
          injectionDuration);
    }

    return shouldInject;
  }

  public Duration getInjectionDuration() {
    return injectionDuration;
  }

  public String getEffectiveDurationParameter() {
    return effectiveDurationParameter;
  }

  public Instant getStartTime() {
    return startTime;
  }

  void setClockForTesting(Clock clock) {
    this.clock = clock;
  }

  @Override
  public String toString() {
    return "InitialLimitedDurationErrorInjectionPolicy{"
        + "startTime="
        + startTime
        + ", injectionDuration="
        + injectionDuration
        + ", effectiveDurationParameter='"
        + effectiveDurationParameter
        + '\''
        + '}';
  }
}
