/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for windowed rate-limited logging to prevent flooding Cloud Logging while accurately
 * capturing error counts.
 *
 * <p>Ensures that metrics counters are called unconditionally while log messages are throttled to a
 * configurable window (default 30 seconds) using a bounded ConcurrentHashMap.
 */
public class ThrottledLogger implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ThrottledLogger.class);
  public static final long DEFAULT_THROTTLE_INTERVAL_MS = 30_000L;
  private static final int MAX_ERROR_CATEGORIES = 200;

  private final String componentName;
  private final long throttleIntervalMs;
  private transient volatile AtomicLong totalErrors;
  private transient volatile AtomicLong totalRetryable;
  private transient volatile AtomicLong totalSevere;
  private transient volatile ConcurrentHashMap<String, AtomicLong> errorCategories;
  private transient volatile ConcurrentHashMap<String, LogEntryState> logStates;
  private transient volatile AtomicLong lastLogTimestamp;

  public ThrottledLogger() {
    this("ThrottledLogger", DEFAULT_THROTTLE_INTERVAL_MS);
  }

  public ThrottledLogger(String componentName) {
    this(componentName, DEFAULT_THROTTLE_INTERVAL_MS);
  }

  public ThrottledLogger(long throttleIntervalMs) {
    this("ThrottledLogger", throttleIntervalMs);
  }

  public ThrottledLogger(String componentName, long throttleIntervalMs) {
    this.componentName = componentName != null ? componentName : "ThrottledLogger";
    this.throttleIntervalMs =
        throttleIntervalMs > 0 ? throttleIntervalMs : DEFAULT_THROTTLE_INTERVAL_MS;
    init();
  }

  private void init() {
    this.totalErrors = new AtomicLong(0);
    this.totalRetryable = new AtomicLong(0);
    this.totalSevere = new AtomicLong(0);
    this.errorCategories = new ConcurrentHashMap<>();
    this.logStates = new ConcurrentHashMap<>();
    this.lastLogTimestamp = new AtomicLong(System.currentTimeMillis());
  }

  private void ensureInitialized() {
    if (this.totalErrors == null) {
      synchronized (this) {
        if (this.totalErrors == null) {
          init();
        }
      }
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, ClassNotFoundException {
    in.defaultReadObject();
    init();
  }

  public void recordRetryableError(String category, String message) {
    ensureInitialized();
    totalErrors.incrementAndGet();
    totalRetryable.incrementAndGet();
    incrementCategory(category);
    checkAndFlush();
  }

  public void recordSevereError(String category, String message) {
    ensureInitialized();
    totalErrors.incrementAndGet();
    totalSevere.incrementAndGet();
    incrementCategory(category);
    checkAndFlush();
  }

  public void recordError(String category, String message) {
    recordRetryableError(category, message);
  }

  private void incrementCategory(String category) {
    if (errorCategories.size() < MAX_ERROR_CATEGORIES || errorCategories.containsKey(category)) {
      errorCategories.computeIfAbsent(category, k -> new AtomicLong(0)).incrementAndGet();
    }
  }

  private void checkAndFlush() {
    long now = System.currentTimeMillis();
    long last = lastLogTimestamp.get();
    if (now - last >= throttleIntervalMs) {
      if (lastLogTimestamp.compareAndSet(last, now)) {
        flushSummary();
      }
    }
  }

  public void flushSummary() {
    ensureInitialized();
    long errors = totalErrors.getAndSet(0);
    if (errors == 0) {
      return;
    }
    long retryable = totalRetryable.getAndSet(0);
    long severe = totalSevere.getAndSet(0);
    StringBuilder sb = new StringBuilder();
    errorCategories.forEach(
        (cat, count) -> {
          long val = count.getAndSet(0);
          if (val > 0) {
            if (sb.length() > 0) {
              sb.append(", ");
            }
            sb.append(cat).append("=").append(val);
          }
        });

    LOG.warn(
        "[{}] Error Summary (throttled): Total={}, Retryable={}, Severe={}. Breakdown: [{}]",
        componentName,
        errors,
        retryable,
        severe,
        sb);
  }

  public long getTotalErrors() {
    ensureInitialized();
    return totalErrors.get();
  }

  public long getTotalRetryable() {
    ensureInitialized();
    return totalRetryable.get();
  }

  public long getTotalSevere() {
    ensureInitialized();
    return totalSevere.get();
  }

  /** Evaluates if a log message should be emitted for the key in this window. */
  public boolean shouldLog(String key) {
    ensureInitialized();
    long now = System.currentTimeMillis();
    if (logStates.size() >= MAX_ERROR_CATEGORIES) {
      logStates.clear();
    }
    LogEntryState state = logStates.computeIfAbsent(key, k -> new LogEntryState(0));
    long lastTime = state.lastLoggedTimeMs.get();
    if (now - lastTime >= throttleIntervalMs) {
      if (state.lastLoggedTimeMs.compareAndSet(lastTime, now)) {
        return true;
      }
    }
    state.suppressedCount.incrementAndGet();
    return false;
  }

  public long getAndResetSuppressedCount(String key) {
    ensureInitialized();
    LogEntryState state = logStates.get(key);
    return state != null ? state.suppressedCount.getAndSet(0) : 0;
  }

  public void logInfo(Logger logger, String key, String message, Object... args) {
    if (shouldLog(key)) {
      long suppressed = getAndResetSuppressedCount(key);
      if (suppressed > 0) {
        logger.info(
            message
                + " [Suppressed "
                + suppressed
                + " similar logs in the last "
                + (throttleIntervalMs / 1000)
                + "s]",
            args);
      } else {
        logger.info(message, args);
      }
    }
  }

  public void logWarn(Logger logger, String key, String message, Object... args) {
    if (shouldLog(key)) {
      long suppressed = getAndResetSuppressedCount(key);
      if (suppressed > 0) {
        logger.warn(
            message
                + " [Suppressed "
                + suppressed
                + " similar logs in the last "
                + (throttleIntervalMs / 1000)
                + "s]",
            args);
      } else {
        logger.warn(message, args);
      }
    }
  }

  public void logError(Logger logger, String key, String message, Object... args) {
    if (shouldLog(key)) {
      long suppressed = getAndResetSuppressedCount(key);
      if (suppressed > 0) {
        logger.error(
            message
                + " [Suppressed "
                + suppressed
                + " similar logs in the last "
                + (throttleIntervalMs / 1000)
                + "s]",
            args);
      } else {
        logger.error(message, args);
      }
    }
  }

  public void logWarn(Logger logger, Counter counter, String key, String message, Object... args) {
    if (counter != null) {
      counter.inc();
    }
    logWarn(logger, key, message, args);
  }

  public void logError(Logger logger, Counter counter, String key, String message, Object... args) {
    if (counter != null) {
      counter.inc();
    }
    logError(logger, key, message, args);
  }

  public static class LogEntryState implements Serializable {
    private final AtomicLong lastLoggedTimeMs;
    private final AtomicLong suppressedCount;

    public LogEntryState(long initialTimeMs) {
      this.lastLoggedTimeMs = new AtomicLong(initialTimeMs);
      this.suppressedCount = new AtomicLong(0);
    }

    public AtomicLong getLastLoggedTimeMs() {
      return lastLoggedTimeMs;
    }

    public AtomicLong getSuppressedCount() {
      return suppressedCount;
    }
  }
}
