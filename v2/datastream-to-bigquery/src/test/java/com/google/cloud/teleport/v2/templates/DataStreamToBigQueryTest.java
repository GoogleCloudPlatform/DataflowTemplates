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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataStreamToBigQuery}. */
@RunWith(JUnit4.class)
public class DataStreamToBigQueryTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuildBigQueryStorageWrite_schemaUpdateOptionsConfigured() {
    Set<String> fieldsToIgnore = Collections.emptySet();
    BigQueryIO.Write<KV<String, TableRow>> write =
        DataStreamToBigQuery.buildBigQueryStorageWrite(fieldsToIgnore);

    DisplayData displayData = DisplayData.from(write);
    Map<String, Item> items = new HashMap<>();
    for (Item item : displayData.items()) {
      items.put(item.getKey(), item);
    }

    // Storage Write API dynamically updates schema when schemaUpdateOptions is populated
    // without requiring autoSchemaUpdate=true (which is restricted to ignoreUnknownValues=true).
    assertTrue(items.containsKey("schemaUpdateOptions"));
    assertEquals(
        "[ALLOW_FIELD_ADDITION, ALLOW_FIELD_RELAXATION]",
        String.valueOf(items.get("schemaUpdateOptions").getValue()));
    assertTrue(items.containsKey("createDisposition"));
    assertEquals("CREATE_NEVER", String.valueOf(items.get("createDisposition").getValue()));
    assertTrue(items.containsKey("writeDisposition"));
    assertEquals("WRITE_APPEND", String.valueOf(items.get("writeDisposition").getValue()));
  }

  @Test
  public void testRemoveTableRowFields() {
    TableRow row = new TableRow();
    row.set("id", 123);
    row.set("name", "foobar");
    row.set("_metadata_deleted", true);

    TableRow cleanedRow =
        DataStreamToBigQuery.removeTableRowFields(row, ImmutableSet.of("_metadata_deleted"));

    assertEquals(123, cleanedRow.get("id"));
    assertEquals("foobar", cleanedRow.get("name"));
    assertFalse(cleanedRow.containsKey("_metadata_deleted"));

    // Ensure original row was not mutated
    assertTrue(row.containsKey("_metadata_deleted"));
  }

  @Test
  public void testBuildBigQueryStorageWrite_pipelineGraphConstruction() {
    pipeline.enableAbandonedNodeEnforcement(false);
    PCollection<KV<String, TableRow>> input =
        pipeline.apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of())));

    // Applying write with Storage Write API and schemaUpdateOptions must succeed.
    input.apply(
        "WriteStorage",
        DataStreamToBigQuery.buildBigQueryStorageWrite(Collections.emptySet())
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStorageWrite_incompatibleWithIgnoreUnknownValues() {
    pipeline.enableAbandonedNodeEnforcement(false);
    // In Beam, if ignoreUnknownValues is set alongside schemaUpdateOptions in Storage Write API,
    // an IllegalArgumentException is thrown. This test confirms that invariant.
    BigQueryIO.Write<KV<String, TableRow>> write =
        DataStreamToBigQuery.buildBigQueryStorageWrite(Collections.emptySet())
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .ignoreUnknownValues();

    PCollection<KV<String, TableRow>> input =
        pipeline.apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of())));
    input.apply("InvalidWrite", write);
  }
}
