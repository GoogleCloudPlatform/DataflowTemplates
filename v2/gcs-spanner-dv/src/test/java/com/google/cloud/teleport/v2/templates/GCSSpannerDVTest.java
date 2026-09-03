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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GCSSpannerDV} table configuration flows. */
public class GCSSpannerDVTest {

  private GCSSpannerDV.Options options;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(GCSSpannerDV.Options.class);
    // Set required options to bypass early validation (if any)
    options.setGcsInputDirectory("gs://dummy/input");
    options.setProjectId("test-project");
    options.setInstanceId("test-instance");
    options.setDatabaseId("test-database");
    options.setBigQueryDataset("test_dataset");
  }

  @Test
  public void testRunThrowsExceptionWhenBothTableConfigsProvided() {
    options.setTables("table1,table2");
    options.setTableListFilePath("gs://dummy/tables.json");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> GCSSpannerDV.run(options));

    assertTrue(
        thrown.getMessage().contains("Please configure only one of these parameters at a time"));
  }

  @Test
  public void testRunThrowsExceptionWhenTableListFileFailsToRead() {
    options.setTableListFilePath("non_existent_file.json");

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> GCSSpannerDV.run(options));

    assertTrue(thrown.getMessage().contains("Failed to read JSON tableListFilePath"));
  }
}
