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
package com.google.cloud.teleport.v2.clickhouse;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.clickhouse.templates.BigQueryToClickHouse;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link BigQueryToClickHouse}. */
public class BigQueryToClickHouseTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();
  private static final TableRow tableRow =
      new TableRow().set("id", "007").set("state", "CA").set("price", 26.23);

  /**
   * Tests that the {@link BigQueryToClickHouse} pipeline throws an {@link IllegalArgumentException}
   * when no query or input table spec is provided.
   */
  @Test
  public void testNoQueryOrInputTableSpec() {
    exceptionRule.expect(IllegalArgumentException.class);

    // Build pipeline
    TestPipeline pipeline = TestPipeline.create();
    pipeline.apply("CreateInput", Create.of(tableRow));

    // Execute pipeline
    pipeline.run();
  }
}
