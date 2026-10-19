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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeFalse;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/**
 * Tests for {@link SkipRerunOfGenuineFailuresRule}. Each test uses its own test name, because the
 * rule remembers genuine failures per test for the lifetime of the JVM.
 */
@RunWith(JUnit4.class)
public class SkipRerunOfGenuineFailuresRuleTest {

  /** Fake test class used only for its name in {@link Description}s. */
  private static final class FakeIT {}

  @After
  public void clearEnabledProperty() {
    System.clearProperty(SkipRerunOfGenuineFailuresRule.ENABLED_PROPERTY);
  }

  @Test
  public void disabled_returnsBaseStatementUnchanged() {
    assumeFalse(
        "Rule enabled through the environment",
        Boolean.parseBoolean(System.getenv(SkipRerunOfGenuineFailuresRule.ENABLED_ENV)));
    Statement base = new CountingStatement(null);

    Statement statement =
        SkipRerunOfGenuineFailuresRule.fromSystemProperties()
            .apply(base, description("disabled_returnsBaseStatementUnchanged"));

    assertThat(statement).isSameInstanceAs(base);
  }

  @Test
  public void genuineFailure_isNotRerun() throws Throwable {
    AssertionError genuine = new AssertionError("expected 5 rows but was 4");
    CountingStatement base = new CountingStatement(genuine);
    Statement statement = enabledRule().apply(base, description("genuineFailure_isNotRerun"));

    AssertionError firstRun = assertThrows(AssertionError.class, statement::evaluate);
    AssertionError rerun = assertThrows(AssertionError.class, statement::evaluate);

    assertThat(firstRun).isSameInstanceAs(genuine);
    assertThat(base.runs.get()).isEqualTo(1);
    assertThat(rerun)
        .hasMessageThat()
        .startsWith(SkipRerunOfGenuineFailuresRule.NOT_RERUN_MESSAGE_PREFIX);
    assertThat(rerun).hasMessageThat().contains("expected 5 rows but was 4");
    assertThat(rerun).hasCauseThat().isSameInstanceAs(genuine);
  }

  @Test
  public void transientFailure_isRerun() throws Throwable {
    CountingStatement base =
        new CountingStatement(new RuntimeException("ZONE_RESOURCE_POOL_EXHAUSTED"));
    Statement statement = enabledRule().apply(base, description("transientFailure_isRerun"));

    assertThrows(RuntimeException.class, statement::evaluate);
    base.failure = null;
    statement.evaluate();

    assertThat(base.runs.get()).isEqualTo(2);
  }

  @Test
  public void transientFailure_thatFailsAgain_isRerunEachTime() throws Throwable {
    CountingStatement base = new CountingStatement(new RuntimeException("job failed"));
    Statement statement =
        enabledRule().apply(base, description("transientFailure_thatFailsAgain_isRerunEachTime"));

    for (int attempt = 0; attempt < 4; attempt++) {
      RuntimeException failure = assertThrows(RuntimeException.class, statement::evaluate);
      assertThat(failure).hasMessageThat().isEqualTo("job failed");
    }

    assertThat(base.runs.get()).isEqualTo(4);
  }

  @Test
  public void assumptionFailure_isRethrownAndNotRemembered() throws Throwable {
    CountingStatement base = new CountingStatement(new AssumptionViolatedException("skipped"));
    Statement statement =
        enabledRule().apply(base, description("assumptionFailure_isRethrownAndNotRemembered"));

    assertThrows(AssumptionViolatedException.class, statement::evaluate);
    base.failure = null;
    statement.evaluate();

    assertThat(base.runs.get()).isEqualTo(2);
  }

  @Test
  public void passingTest_runsEveryTime() throws Throwable {
    CountingStatement base = new CountingStatement(null);
    Statement statement = enabledRule().apply(base, description("passingTest_runsEveryTime"));

    statement.evaluate();
    statement.evaluate();

    assertThat(base.runs.get()).isEqualTo(2);
  }

  @Test
  public void genuineFailure_onlyAffectsTheSameTest() throws Throwable {
    TestRule rule = enabledRule();
    Statement failing =
        rule.apply(
            new CountingStatement(new AssertionError("wrong")),
            description("genuineFailure_onlyAffectsTheSameTest_failing"));
    CountingStatement otherBase = new CountingStatement(null);
    Statement other =
        rule.apply(otherBase, description("genuineFailure_onlyAffectsTheSameTest_other"));

    assertThrows(AssertionError.class, failing::evaluate);
    other.evaluate();

    assertThat(otherBase.runs.get()).isEqualTo(1);
  }

  private static TestRule enabledRule() {
    System.setProperty(SkipRerunOfGenuineFailuresRule.ENABLED_PROPERTY, "true");
    return SkipRerunOfGenuineFailuresRule.fromSystemProperties();
  }

  private static Description description(String methodName) {
    return Description.createTestDescription(FakeIT.class, methodName);
  }

  /** Statement that counts its runs and throws {@code failure} if it is set. */
  private static final class CountingStatement extends Statement {
    private final AtomicInteger runs = new AtomicInteger();
    private Throwable failure;

    private CountingStatement(Throwable failure) {
      this.failure = failure;
    }

    @Override
    public void evaluate() throws Throwable {
      runs.incrementAndGet();
      if (failure != null) {
        throw failure;
      }
    }
  }
}
