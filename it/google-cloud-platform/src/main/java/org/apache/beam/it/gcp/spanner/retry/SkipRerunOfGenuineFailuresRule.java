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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guard for Surefire's {@code rerunFailingTestsCount}: when a test fails genuinely (see {@link
 * TransientFailureClassifier}), a later rerun of the same test in this JVM rethrows the original
 * failure immediately, without running {@code @Before}, the test or {@code @After}. Transient
 * failures are rerun normally, so Surefire reports them as flaky if they pass.
 *
 * <p>Register it as the outermost rule ({@code @Rule(order = Integer.MIN_VALUE)}) so it also wraps
 * the setup/teardown of parent classes. It is a no-op unless {@code
 * -DitSkipRerunOfGenuineFailures=true} or the environment variable {@code
 * IT_SKIP_RERUN_OF_GENUINE_FAILURES=true} is set. The environment variable lets a workflow enable
 * it without passing extra Maven flags.
 */
public final class SkipRerunOfGenuineFailuresRule implements TestRule {

  public static final String ENABLED_PROPERTY = "itSkipRerunOfGenuineFailures";

  public static final String ENABLED_ENV = "IT_SKIP_RERUN_OF_GENUINE_FAILURES";

  private static final Logger LOG = LoggerFactory.getLogger(SkipRerunOfGenuineFailuresRule.class);

  /** Genuine failures seen in this JVM, keyed by "class#method". */
  private static final Map<String, Throwable> GENUINE_FAILURES = new ConcurrentHashMap<>();

  private final boolean enabled;

  private SkipRerunOfGenuineFailuresRule(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Creates the rule, enabled only if {@code -DitSkipRerunOfGenuineFailures=true} or {@code
   * IT_SKIP_RERUN_OF_GENUINE_FAILURES=true}.
   */
  public static SkipRerunOfGenuineFailuresRule fromSystemProperties() {
    return new SkipRerunOfGenuineFailuresRule(
        Boolean.getBoolean(ENABLED_PROPERTY) || Boolean.parseBoolean(System.getenv(ENABLED_ENV)));
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (!enabled) {
      return base;
    }
    String testKey = description.getClassName() + "#" + description.getMethodName();
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Throwable previousGenuineFailure = GENUINE_FAILURES.get(testKey);
        if (previousGenuineFailure != null) {
          LOG.warn(
              "[IT-RETRY] Not re-running {}: previous attempt failed genuinely: {}",
              testKey,
              previousGenuineFailure.toString());
          throw previousGenuineFailure;
        }
        try {
          base.evaluate();
        } catch (AssumptionViolatedException e) {
          throw e;
        } catch (Throwable t) {
          if (TransientFailureClassifier.isGenuine(t)) {
            GENUINE_FAILURES.put(testKey, t);
            LOG.warn(
                "[IT-RETRY] {} failed genuinely, a rerun will not re-execute it: {}",
                testKey,
                t.toString());
          } else {
            java.util.regex.Pattern matched =
                TransientFailureClassifier.matchingRetryablePattern(t);
            LOG.warn(
                "[IT-RETRY] {} failed with a transient error{}, a rerun will re-execute it: {}",
                testKey,
                matched == null ? "" : " (matched retryable pattern '" + matched.pattern() + "')",
                t.toString());
          }
          throw t;
        }
      }
    };
  }
}
