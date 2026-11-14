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
package com.google.cloud.teleport.v2.spanner.testutils.retry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.runners.model.MultipleFailureException;

/**
 * Classifies a test failure as genuine (the test itself detected a problem, or a programming error)
 * or transient (e.g. infrastructure errors). Genuine failures should not be retried.
 *
 * <p>Order of checks:
 *
 * <ol>
 *   <li>If the message of any throwable in the cause chain matches a retryable pattern ({@link
 *       #DEFAULT_RETRYABLE_MESSAGE_PATTERNS} or {@code -DitRetryableFailureMessageRegex}), the
 *       failure is transient, even if its type is in {@link #GENUINE_FAILURE_TYPES}.
 *   <li>Else, if any throwable in the cause chain is one of {@link #GENUINE_FAILURE_TYPES}, the
 *       failure is genuine.
 *   <li>Else, the failure is transient.
 * </ol>
 *
 * <p>A {@link MultipleFailureException} anywhere in the cause chain is genuine if any of its
 * failures is genuine (each checked with the rules above).
 */
public final class TransientFailureClassifier {

  /** Optional extra regex (Java syntax, matched with find()) for retryable failure messages. */
  public static final String EXTRA_RETRYABLE_REGEX_PROPERTY = "itRetryableFailureMessageRegex";

  /** Throwable types that indicate a genuine failure if found anywhere in the cause chain. */
  private static final List<Class<? extends Throwable>> GENUINE_FAILURE_TYPES =
      List.of(
          AssertionError.class,
          NullPointerException.class,
          IllegalArgumentException.class,
          IllegalStateException.class,
          ClassCastException.class,
          NoClassDefFoundError.class);

  /**
   * Known transient/infrastructure error messages. Matching any of these means: retry. Only errors
   * raised in the test JVM can match (resource managers, Testcontainers, GCP clients); a failed
   * Dataflow/Direct job only surfaces a generic "Job ... failed" message. Based on failed Spanner
   * PR workflow runs (Jul-Oct 2026): signatures that recurred across branches, on main, and passed
   * on re-run.
   */
  private static final List<Pattern> DEFAULT_RETRYABLE_MESSAGE_PATTERNS =
      List.of(
          // Datastream rejecting operations transiently.
          Pattern.compile("(?i)unable to queue"),
          // Datastream long-running operations failing with INTERNAL/UNKNOWN status.
          Pattern.compile("transportCode=(INTERNAL|UNKNOWN)"),
          // Source database connection drops (see CloudSqlResourceManager retries).
          Pattern.compile("Communications link failure"),
          Pattern.compile("The connection attempt failed"),
          // Testcontainers database started but not reachable yet.
          Pattern.compile("Container is started, but cannot be accessed"),
          // Cassandra driver query timeouts (DriverTimeoutException).
          Pattern.compile("Query timed out after"),
          // GCP API unavailability.
          Pattern.compile("503 Service Unavailable"),
          Pattern.compile("code=502"));

  private TransientFailureClassifier() {}

  /** Returns true if {@code failure} is genuine and therefore should not be retried. */
  public static boolean isGenuine(Throwable failure) {
    // Cause chain up to (excluding) the first MultipleFailureException, if any. A
    // MultipleFailureException (e.g. the test failed and @After also failed) may be the failure
    // itself or wrapped in another exception; it is genuine if any of its failures is genuine.
    List<Throwable> outer = new ArrayList<>();
    MultipleFailureException multiple = null;
    for (Throwable t : causeChain(failure)) {
      if (t instanceof MultipleFailureException) {
        multiple = (MultipleFailureException) t;
        break;
      }
      outer.add(t);
    }
    for (Throwable t : outer) {
      if (matchingPattern(t.getMessage()) != null) {
        return false;
      }
    }
    if (multiple != null) {
      for (Throwable t : multiple.getFailures()) {
        if (isGenuine(t)) {
          return true;
        }
      }
    }
    for (Throwable t : outer) {
      for (Class<? extends Throwable> genuineType : GENUINE_FAILURE_TYPES) {
        if (genuineType.isInstance(t)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns the retryable pattern that matches {@code failure}, or null if none matches. */
  public static Pattern matchingRetryablePattern(Throwable failure) {
    for (Throwable t : causeChain(failure)) {
      if (t instanceof MultipleFailureException) {
        // Its message concatenates all failures; check each failure on its own instead.
        for (Throwable inner : ((MultipleFailureException) t).getFailures()) {
          Pattern pattern = matchingRetryablePattern(inner);
          if (pattern != null) {
            return pattern;
          }
        }
        return null;
      }
      Pattern pattern = matchingPattern(t.getMessage());
      if (pattern != null) {
        return pattern;
      }
    }
    return null;
  }

  private static Pattern matchingPattern(String message) {
    if (message == null) {
      return null;
    }
    for (Pattern pattern : retryablePatterns()) {
      if (pattern.matcher(message).find()) {
        return pattern;
      }
    }
    return null;
  }

  private static List<Pattern> retryablePatterns() {
    String extraRegex = System.getProperty(EXTRA_RETRYABLE_REGEX_PROPERTY);
    if (extraRegex == null || extraRegex.isEmpty()) {
      return DEFAULT_RETRYABLE_MESSAGE_PATTERNS;
    }
    List<Pattern> patterns = new ArrayList<>(DEFAULT_RETRYABLE_MESSAGE_PATTERNS);
    patterns.add(Pattern.compile(extraRegex));
    return patterns;
  }

  private static List<Throwable> causeChain(Throwable failure) {
    List<Throwable> chain = new ArrayList<>();
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }
}
