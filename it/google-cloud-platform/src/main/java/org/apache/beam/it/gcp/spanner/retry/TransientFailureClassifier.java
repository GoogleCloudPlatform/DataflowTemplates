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
package org.apache.beam.it.gcp.spanner.retry;

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

  /** Known transient/infrastructure error messages. Matching any of these means: retry. */
  private static final List<Pattern> DEFAULT_RETRYABLE_MESSAGE_PATTERNS =
      List.of(
          // Dataflow / GCE stockouts.
          Pattern.compile("ZONE_RESOURCE_POOL_EXHAUSTED"),
          Pattern.compile("does not have enough resources available"),
          // Datastream rejecting operations transiently.
          Pattern.compile("(?i)unable to queue"),
          // Source database connection drops (see CloudSqlResourceManager retries).
          Pattern.compile("Communications link failure"),
          Pattern.compile("The connection attempt failed"));

  private TransientFailureClassifier() {}

  /** Returns true if {@code failure} is genuine and therefore should not be retried. */
  public static boolean isGenuine(Throwable failure) {
    if (failure instanceof MultipleFailureException) {
      // e.g. the test failed and @After also failed: genuine if any of them is genuine.
      for (Throwable t : ((MultipleFailureException) failure).getFailures()) {
        if (isGenuine(t)) {
          return true;
        }
      }
      return false;
    }
    List<Throwable> chain = causeChain(failure);
    if (matchesRetryableMessage(chain)) {
      return false;
    }
    for (Throwable t : chain) {
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
      String message = t.getMessage();
      if (message == null) {
        continue;
      }
      for (Pattern pattern : retryablePatterns()) {
        if (pattern.matcher(message).find()) {
          return pattern;
        }
      }
    }
    return null;
  }

  private static boolean matchesRetryableMessage(List<Throwable> chain) {
    for (Throwable t : chain) {
      if (matchingRetryablePattern(t) != null) {
        return true;
      }
    }
    return false;
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
