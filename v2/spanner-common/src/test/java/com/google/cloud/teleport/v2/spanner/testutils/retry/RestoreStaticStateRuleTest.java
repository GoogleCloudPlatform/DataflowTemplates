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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link RestoreStaticStateRule}. */
@RunWith(JUnit4.class)
public class RestoreStaticStateRuleTest {

  /** Fake IT holding shared per-class state in static fields, like the Live/Reverse ITs. */
  private static final class FakeIT {
    private static String jobInfo;
    private static int launches;
    private static String ddlResource = "ddl.sql";
    private static final Set<String> TEST_INSTANCES = new HashSet<>();
    private static Map<String, String> shards = new HashMap<>(Map.of("shard1", "db1"));
    private static final String CONSTANT = "constant";
  }

  /** Fake subclass: the rule must only restore fields declared by the test class itself. */
  private static final class FakeSubIT {
    private static String subJobInfo;
  }

  @After
  public void clearEnabledProperty() {
    System.clearProperty(SkipRerunOfGenuineFailuresRule.ENABLED_PROPERTY);
  }

  private static Statement runClass() {
    return new Statement() {
      @Override
      public void evaluate() {
        FakeIT.jobInfo = "job-1";
        FakeIT.launches++;
        FakeIT.ddlResource = "other.sql";
        FakeIT.TEST_INSTANCES.add("instance-1");
        FakeIT.shards.put("shard2", "db2");
        FakeIT.shards = new HashMap<>();
      }
    };
  }

  private static Description classDescription(Class<?> testClass) {
    return Description.createSuiteDescription(testClass);
  }

  @Test
  public void restoresStaticFieldsAfterTheClassRan() throws Throwable {
    Map<String, String> originalShards = FakeIT.shards;

    RestoreStaticStateRule.create().apply(runClass(), classDescription(FakeIT.class)).evaluate();

    assertThat(FakeIT.jobInfo).isNull();
    assertThat(FakeIT.launches).isEqualTo(0);
    assertThat(FakeIT.ddlResource).isEqualTo("ddl.sql");
    assertThat(FakeIT.TEST_INSTANCES).isEmpty();
    assertThat(FakeIT.shards).isSameInstanceAs(originalShards);
    assertThat(FakeIT.shards).containsExactly("shard1", "db1");
    assertThat(FakeIT.CONSTANT).isEqualTo("constant");
  }

  @Test
  public void restoresStaticFieldsWhenTheClassFails() {
    Statement failing =
        new Statement() {
          @Override
          public void evaluate() throws Throwable {
            runClass().evaluate();
            throw new IllegalStateException("class failed");
          }
        };

    assertThrows(
        IllegalStateException.class,
        () ->
            RestoreStaticStateRule.create()
                .apply(failing, classDescription(FakeIT.class))
                .evaluate());

    assertThat(FakeIT.jobInfo).isNull();
    assertThat(FakeIT.TEST_INSTANCES).isEmpty();
  }

  @Test
  public void onlyRestoresFieldsOfTheDescribedClass() throws Throwable {
    Statement setsBoth =
        new Statement() {
          @Override
          public void evaluate() {
            FakeIT.jobInfo = "other-class-job";
            FakeSubIT.subJobInfo = "job-1";
          }
        };

    RestoreStaticStateRule.create().apply(setsBoth, classDescription(FakeSubIT.class)).evaluate();

    assertThat(FakeSubIT.subJobInfo).isNull();
    assertThat(FakeIT.jobInfo).isEqualTo("other-class-job");
    FakeIT.jobInfo = null;
  }

  @Test
  public void noTestClass_returnsBaseStatementUnchanged() {
    Statement base = runClass();

    Statement statement =
        RestoreStaticStateRule.create().apply(base, Description.createSuiteDescription("suite"));

    assertThat(statement).isSameInstanceAs(base);
  }

  @Test
  public void fromSystemProperties_disabledByDefault_returnsBaseStatementUnchanged() {
    assumeFalse(Boolean.parseBoolean(System.getenv(SkipRerunOfGenuineFailuresRule.ENABLED_ENV)));
    Statement base = runClass();

    Statement statement =
        RestoreStaticStateRule.fromSystemProperties().apply(base, classDescription(FakeIT.class));

    assertThat(statement).isSameInstanceAs(base);
  }

  @Test
  public void fromSystemProperties_enabledByProperty_restoresStaticFields() throws Throwable {
    System.setProperty(SkipRerunOfGenuineFailuresRule.ENABLED_PROPERTY, "true");

    RestoreStaticStateRule.fromSystemProperties()
        .apply(runClass(), classDescription(FakeIT.class))
        .evaluate();

    assertThat(FakeIT.jobInfo).isNull();
    assertThat(FakeIT.TEST_INSTANCES).isEmpty();
  }
}
