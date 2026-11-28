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

import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.junit.Test;

public class CheckFakeDatasetTest {
  @Test
  public void check() {
    java.lang.reflect.Method[] methods = FakeDatasetService.class.getMethods();
    for (java.lang.reflect.Method m : methods) {
      System.out.println("METHOD: " + m.getName() + " -> " + m.toGenericString());
    }
  }
}
