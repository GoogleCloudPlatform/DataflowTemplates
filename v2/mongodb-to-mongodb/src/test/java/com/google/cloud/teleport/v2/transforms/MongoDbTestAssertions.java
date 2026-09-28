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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/** Assertions shared across the MongoDB to MongoDB template's tests. */
final class MongoDbTestAssertions {

  private MongoDbTestAssertions() {}

  /**
   * Asserts that the named methods of {@code trackerClass} are declared {@code synchronized}.
   *
   * <p>{@link org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker} documents that
   * {@code trySplit} and {@code getProgress} may be invoked from a different thread than the one
   * running {@code tryClaim}, so any method touching a tracker's mutable state must hold the
   * tracker's monitor. This is asserted on the declaration rather than by a stress test because the
   * underlying data race cannot be reproduced deterministically.
   *
   * @param trackerClass the tracker implementation to inspect
   * @param stateAccessors names of the methods that read or write the tracker's mutable state;
   *     methods that touch no state (for example a no-op {@code checkDone}) should be omitted
   */
  static void assertStateAccessorsSynchronized(
      Class<?> trackerClass, ImmutableList<String> stateAccessors) {
    for (Method method : trackerClass.getDeclaredMethods()) {
      // Generic superclasses produce unsynchronized bridge methods that merely delegate.
      if (method.isBridge() || method.isSynthetic() || !stateAccessors.contains(method.getName())) {
        continue;
      }
      assertTrue(
          trackerClass.getSimpleName() + "." + method.getName() + " must be synchronized",
          Modifier.isSynchronized(method.getModifiers()));
    }
  }
}
