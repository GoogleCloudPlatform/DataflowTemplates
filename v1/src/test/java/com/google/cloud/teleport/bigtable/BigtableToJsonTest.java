/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.BigtableToJson.BigtableToJsonFn;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;

import com.google.bigtable.v2.Row;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigtableToJson}. */
@RunWith(JUnit4.class)
public final class BigtableToJsonTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBigtableToJsonUnflattened() throws Exception {
    String expectedJson =
        "{\"row1\":{\"family1\":{\"column1\":\"value1\",\"column2\":\"value2\",\"column3\":\"value3\"}}}";

    testBigtableToJson(expectedJson, false, "");
  }

  @Test
  public void testBigtableToJsonFlattened() throws Exception {
    String expectedJson =
        "{\"rowkey\":\"row1\",\"family1:column1\":\"value1\",\"family1:column2\":\"value2\",\"family1:column3\":\"value3\"}";

    testBigtableToJson(expectedJson, true, "");
  }

  @Test
  public void testBigtableToJsonFlattenedWithAliases() throws Exception {
    String expectedJson = "{\"id\":\"row1\",\"alias\":\"value1\",\"something_else\":\"value3\"}";

    testBigtableToJson(
        expectedJson, true, "rowkey;id,family1:column1;alias,family1:column3;something_else");
  }

  private void testBigtableToJson(String expectedJson, boolean flattened, String columnsAliases)
      throws Exception {
    Row row = createBigtableRow("row1");
    row = upsertBigtableCell(row, "family1", "column1", 1, "value1");
    row = upsertBigtableCell(row, "family1", "column2", 1, "value2");
    row = upsertBigtableCell(row, "family1", "column3", 1, "value3");

    PCollection<String> jsonRows =
        pipeline
            .apply("Create", Create.of(row))
            .apply(
                "Transform to JSON",
                MapElements.via(
                    new BigtableToJsonFn(flattened, StaticValueProvider.of(columnsAliases))))
            .setCoder(StringUtf8Coder.of());

    // Assert on the jsonRows.
    PAssert.that(jsonRows).containsInAnyOrder(expectedJson);

    pipeline.run();
  }
}
