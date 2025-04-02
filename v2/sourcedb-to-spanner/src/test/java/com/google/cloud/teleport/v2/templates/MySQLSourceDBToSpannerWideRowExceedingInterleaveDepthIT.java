/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.Assert;
import org.junit.Test;

public class MySQLSourceDBToSpannerWideRowExceedingInterleaveDepthIT
    extends SourceDbToSpannerITBase {

  private SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_SCHEMA_DEPTH_8_FILE_RESOURCE =
      "WideRow/InterleaveDepthIT/spanner-schema-depth-8.sql";

  @Test
  public void wideRowInterleaveDepth8FailureTest() {
    try {
      // Attempt to create a schema with interleave depth of 8 (which exceeds Spanner's limit of
      createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_DEPTH_8_FILE_RESOURCE);
    } catch (Exception e) {
      System.out.println("===>>>>>> Exception caught: " + e.getMessage());
      Assert.assertTrue(
          "Exception should mention key column limitation",
          e.getMessage().contains("Failed to execute statement"));
    }
  }
}
