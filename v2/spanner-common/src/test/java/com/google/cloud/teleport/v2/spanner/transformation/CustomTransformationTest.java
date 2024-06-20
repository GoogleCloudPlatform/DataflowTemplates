/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.transformation;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CustomTransformationTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();
  private CustomTransformation customTransformation;

  @Test
  public void testBuildFailure() {
    expectedEx.expect(IllegalStateException.class);
    expectedEx.expectMessage("Both jarPath and classPath must be set or both must be empty/null.");
    CustomTransformation.builder("", "sampleClassPath")
        .setCustomParameters("customParam=xyz")
        .build();
  }

  @Test
  public void testBuildSuccess() {
    customTransformation =
        CustomTransformation.builder("sampleJarPath", "sampleClassPath")
            .setCustomParameters("customParam=xyz")
            .build();
    assertEquals(customTransformation.classPath(), "sampleClassPath");
    assertEquals(customTransformation.jarPath(), "sampleJarPath");
    assertEquals(customTransformation.customParameters(), "customParam=xyz");
  }
}
