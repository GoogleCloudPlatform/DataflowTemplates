/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit Tests for {@link BigQueryConverters}. */
@RunWith(JUnit4.class)
public class BigQueryConvertersTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * Tests {@link BigQueryConverters.JsonToTableRow} converts a valid Json TableRow to a TableRow.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testJsonToTableRowGood() throws Exception {
    TableRow expectedRow = new TableRow();
    expectedRow.set("location", "nyc");
    expectedRow.set("name", "Adam");
    expectedRow.set("age", 30);
    expectedRow.set("color", "blue");
    expectedRow.set("coffee", "black");

    ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
    TableRowJsonCoder.of().encode(expectedRow, jsonStream, Context.OUTER);
    String expectedJson = new String(jsonStream.toByteArray(), StandardCharsets.UTF_8.name());

    PCollection<TableRow> transformedJson = pipeline
        .apply("Create", Create.of(expectedJson))
        .apply(BigQueryConverters.jsonToTableRow());

    PAssert.that(transformedJson).containsInAnyOrder(expectedRow);

    pipeline.run();
  }
}
