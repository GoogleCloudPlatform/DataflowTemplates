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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("This test is completed")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDbToSpannerWideRowExceedingMaxColumnsTableKeyIT
    extends SourceDbToSpannerITBase {

  private static SpannerResourceManager spannerResourceManager;
  private static final String SPANNER_SCHEMA_EXCEEDING_KEYS_FILE_RESOURCE =
      "WideRow/MaxColumnsTableKeyIT/spanner-schema-exceeding-keys.sql";

  @Before
  public void setUp() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void wideRowExceedingMaxColumnsTableKeyTest() {
    try {
      createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_EXCEEDING_KEYS_FILE_RESOURCE);
    } catch (Exception e) {
      Assert.assertTrue(
          "Exception should mention key column limitation",
          e.getMessage().contains("Failed to execute statement"));
    }
  }
}
