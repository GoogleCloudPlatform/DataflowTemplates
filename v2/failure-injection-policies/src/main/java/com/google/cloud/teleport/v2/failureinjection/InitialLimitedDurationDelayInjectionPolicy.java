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
 * An {@link ErrorInjectionPolicy} that injects a fixed delay for a specified initial duration.
 *
 * <p>After the initial duration has passed, it will no longer signal for delays to be injected.
 */
public class InitialLimitedDurationDelayInjectionPolicy
    implements ErrorInjectionPolicy, Serializable {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialLimitedDurationDelayInjectionPolicy.class);
  private static final long serialVersionUID = 1L;

  private static final String DEFAULT_INJECTION_WINDOW_DURATION = "PT10M"; // 10 minutes
  private static final String DEFAULT_DELAY_DURATION = "PT10S"; // 10 seconds
  private static final String INJECTION_WINDOW_FIELD = "injectionWindowDuration";
  private static final String DELAY_DURATION_FIELD = "delayDuration";

  private final Duration injectionWindowDuration;
  private final Duration delayDuration;

  private transient Clock clock;
  private Instant startTime;

  public InitialLimitedDurationDelayInjectionPolicy(JsonNode inputParameter) {
    this(inputParameter, Clock.systemUTC());
  }

  InitialLimitedDurationDelayInjectionPolicy(JsonNode inputParameter, Clock clock) {
    this.clock = clock;
    String injectionWindowStr = DEFAULT_INJECTION_WINDOW_DURATION;
    String delayDurationStr = DEFAULT_DELAY_DURATION;

    if (inputParameter != null && inputParameter.isObject()) {
      injectionWindowStr =
          inputParameter.has(INJECTION_WINDOW_FIELD)
              ? inputParameter.get(INJECTION_WINDOW_FIELD).asText(injectionWindowStr)
              : injectionWindowStr;
      delayDurationStr =
          inputParameter.has(DELAY_DURATION_FIELD)
              ? inputParameter.get(DELAY_DURATION_FIELD).asText(delayDurationStr)
              : delayDurationStr;
    }

    this.injectionWindowDuration = parseDuration(injectionWindowStr, INJECTION_WINDOW_FIELD);
    this.delayDuration = parseDuration(delayDurationStr, DELAY_DURATION_FIELD);
  }

  @Override
  public boolean shouldInjectionError() {
    if (this.startTime == null) {
      synchronized (this) {
        if (this.startTime == null) {
          this.startTime = Instant.now(clock);
          LOG.info(
              "First call detected. Delays will be injected for {} starting from {}.",
              this.injectionWindowDuration,
              this.startTime);
        }
      }
    }

    Instant now = Instant.now(clock);
    Duration elapsed = Duration.between(startTime, now);

    // Compare elapsed time with the configured duration.
    // elapsed.compareTo(injectionWindowDuration) < 0 means elapsed < injectionWindowDuration
    if (elapsed.compareTo(injectionWindowDuration) < 0) {
      // Introduce delay
      try {
        Thread.sleep(delayDuration.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Delay injection interrupted", e);
      }
    }

    return false;
  }

  private static Duration parseDuration(String durationStr, String fieldName) {
    try {
      return Duration.parse(durationStr);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Failed to parse "
              + fieldName
              + ". Expected ISO-8601 format (e.g., 'PT10M'). Value: "
              + durationStr,
          e);
    }
  }

  public Duration getDelay() {
    return this.delayDuration;
  }

  public void setClockForTesting(Clock clock) {
    this.clock = clock;
  }
}
