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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class rule that restores the non-final static fields declared by the test class to the values
 * they had before the class ran, after {@code @AfterClass} has finished.
 *
 * <p>Many ITs share one Dataflow job and its resources across the methods of a class: the first
 * {@code @Before} creates them and stores them in static fields (e.g. {@code if (jobInfo == null)
 * jobInfo = launchDataflowJob(...)}) and {@code @AfterClass} tears them down without clearing the
 * fields. Surefire's {@code rerunFailingTestsCount} re-runs failed methods in the same JVM after
 * the class has finished, so without this rule a rerun would skip the setup and use the torn-down
 * job and resources. With it, a rerun starts from the same state as the first run: it creates new
 * resources and launches a new job for the re-run methods.
 *
 * <p>Only fields declared by the test class itself are restored, not those of its superclasses, so
 * state shared by all test classes (e.g. a shared database container in a base class) is kept.
 * Collections and maps held by static fields (final or not, e.g. the {@code testInstances} set) are
 * restored to their previous contents.
 *
 * <p>Use {@link #fromSystemProperties()} so the rule only acts when retries are enabled the same
 * way as {@link SkipRerunOfGenuineFailuresRule} ({@code -DitSkipRerunOfGenuineFailures=true} or
 * {@code IT_SKIP_RERUN_OF_GENUINE_FAILURES=true}); otherwise it is a no-op.
 */
public final class RestoreStaticStateRule implements TestRule {

  private static final Logger LOG = LoggerFactory.getLogger(RestoreStaticStateRule.class);

  private final boolean enabled;

  private RestoreStaticStateRule(boolean enabled) {
    this.enabled = enabled;
  }

  /** Creates the rule, always enabled. */
  public static RestoreStaticStateRule create() {
    return new RestoreStaticStateRule(true);
  }

  /**
   * Creates the rule, enabled only if {@code -DitSkipRerunOfGenuineFailures=true} or {@code
   * IT_SKIP_RERUN_OF_GENUINE_FAILURES=true}.
   */
  public static RestoreStaticStateRule fromSystemProperties() {
    return new RestoreStaticStateRule(
        Boolean.getBoolean(SkipRerunOfGenuineFailuresRule.ENABLED_PROPERTY)
            || Boolean.parseBoolean(System.getenv(SkipRerunOfGenuineFailuresRule.ENABLED_ENV)));
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Class<?> testClass = description.getTestClass();
    if (!enabled || testClass == null) {
      return base;
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        List<SavedField> saved = save(testClass);
        try {
          base.evaluate();
        } finally {
          restore(saved);
        }
      }
    };
  }

  private static List<SavedField> save(Class<?> testClass) {
    List<SavedField> saved = new ArrayList<>();
    for (Field field : testClass.getDeclaredFields()) {
      int modifiers = field.getModifiers();
      if (!Modifier.isStatic(modifiers) || field.isSynthetic()) {
        continue;
      }
      try {
        field.setAccessible(true);
        Object value = field.get(null);
        boolean isFinal = Modifier.isFinal(modifiers);
        if (isFinal && !(value instanceof Collection) && !(value instanceof Map)) {
          continue;
        }
        saved.add(new SavedField(field, value, isFinal));
      } catch (RuntimeException | IllegalAccessException e) {
        LOG.warn("[IT-RETRY] Cannot save static field {}: {}", field, e.toString());
      }
    }
    return saved;
  }

  private static void restore(List<SavedField> saved) {
    for (SavedField savedField : saved) {
      try {
        savedField.restore();
      } catch (RuntimeException | IllegalAccessException e) {
        LOG.warn("[IT-RETRY] Cannot restore static field {}: {}", savedField.field, e.toString());
      }
    }
  }

  /** A static field, its value before the class ran and, for collections/maps, their contents. */
  private static final class SavedField {
    private final Field field;
    private final Object value;
    private final boolean isFinal;
    private final List<Object> collectionContents;
    private final Map<Object, Object> mapContents;

    @SuppressWarnings("unchecked")
    SavedField(Field field, Object value, boolean isFinal) {
      this.field = field;
      this.value = value;
      this.isFinal = isFinal;
      this.collectionContents =
          value instanceof Collection ? new ArrayList<>((Collection<Object>) value) : null;
      this.mapContents =
          value instanceof Map ? new LinkedHashMap<>((Map<Object, Object>) value) : null;
    }

    @SuppressWarnings("unchecked")
    void restore() throws IllegalAccessException {
      if (!isFinal) {
        field.set(null, value);
      }
      // Only touch the contents if they changed, so unmodifiable collections are left alone.
      if (collectionContents != null) {
        Collection<Object> collection = (Collection<Object>) value;
        if (!new ArrayList<>(collection).equals(collectionContents)) {
          collection.clear();
          collection.addAll(collectionContents);
        }
      } else if (mapContents != null) {
        Map<Object, Object> map = (Map<Object, Object>) value;
        if (!map.equals(mapContents)) {
          map.clear();
          map.putAll(mapContents);
        }
      }
    }
  }
}
