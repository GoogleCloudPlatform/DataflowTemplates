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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/** Tests for Schema class. */
public class SchemaTest {

  @Test
  public void getSpannerColumnNamesBasic() {
    Schema schema = getSchemaObject();
    schema.generateMappings();
    List<String> actualColNames = schema.getSpannerColumnNames("new_cart");
    List<String> expectedColNames = Arrays.asList("new_product_id", "new_quantity", "new_user_id");
    assertThat(actualColNames, is(expectedColNames));
  }

  @Test
  public void getPrimaryKeySetBasic() {
    Schema schema = getSchemaObject();
    schema.generateMappings();
    Set<String> actualKeys = schema.getPrimaryKeySet("new_cart");
    Set<String> expectedKeys = new HashSet<>(Arrays.asList("new_user_id", "new_product_id"));
    assertThat(actualKeys, is(expectedKeys));
  }

  private static Schema getSchemaObject() {
    // Add Synthetic PKs.
    Map<String, SyntheticPKey> syntheticPKeys = getSyntheticPks();
    // Add SrcSchema.
    Map<String, SourceTable> srcSchema = getSampleSrcSchema();
    // Add SpSchema.
    Map<String, SpannerTable> spSchema = getSampleSpSchema();
    // Add ToSpanner.
    Map<String, NameAndCols> toSpanner = getToSpanner();
    // Add SrcToID.
    Map<String, NameAndCols> srcToId = getSrcToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setToSpanner(toSpanner);
    expectedSchema.setToSource(new HashMap<String, NameAndCols>());
    expectedSchema.setSrcToID(srcToId);
    expectedSchema.setSpannerToID(new HashMap<String, NameAndCols>());
    return expectedSchema;
  }

  private static Map<String, SyntheticPKey> getSyntheticPks() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    syntheticPKeys.put("t2", new SyntheticPKey("c6", 0));
    return syntheticPKeys;
  }

  private static Map<String, SourceTable> getSampleSrcSchema() {
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SourceColumnDefinition> t1SrcColDefs =
        new HashMap<String, SourceColumnDefinition>();
    t1SrcColDefs.put(
        "c1",
        new SourceColumnDefinition(
            "product_id", new SourceColumnType("varchar", new Long[] {20L}, null)));
    t1SrcColDefs.put(
        "c2", new SourceColumnDefinition("quantity", new SourceColumnType("bigint", null, null)));
    t1SrcColDefs.put(
        "c3",
        new SourceColumnDefinition(
            "user_id", new SourceColumnType("varchar", new Long[] {20L}, null)));
    srcSchema.put(
        "t1",
        new SourceTable(
            "cart",
            "my_schema",
            new String[] {"c3", "c1", "c2"},
            t1SrcColDefs,
            new ColumnPK[] {new ColumnPK("c3", 1), new ColumnPK("c1", 2)}));
    Map<String, SourceColumnDefinition> t2SrcColDefs =
        new HashMap<String, SourceColumnDefinition>();
    t2SrcColDefs.put(
        "c5",
        new SourceColumnDefinition(
            "name", new SourceColumnType("varchar", new Long[] {20L}, null)));
    srcSchema.put(
        "t2", new SourceTable("people", "my_schema", new String[] {"c5"}, t2SrcColDefs, null));
    return srcSchema;
  }

  private static Map<String, SpannerTable> getSampleSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1",
        new SpannerColumnDefinition("new_product_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("new_quantity", new SpannerColumnType("INT64", false)));
    t1SpColDefs.put(
        "c3", new SpannerColumnDefinition("new_user_id", new SpannerColumnType("STRING", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "new_cart",
            new String[] {"c1", "c2", "c3"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c3", 1), new ColumnPK("c1", 2)},
            ""));
    Map<String, SpannerColumnDefinition> t2SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t2SpColDefs.put(
        "c5", new SpannerColumnDefinition("new_name", new SpannerColumnType("STRING", false)));
    t2SpColDefs.put(
        "c6", new SpannerColumnDefinition("synth_id", new SpannerColumnType("INT64", false)));
    spSchema.put(
        "t2",
        new SpannerTable(
            "new_people",
            new String[] {"c5", "c6"},
            t2SpColDefs,
            new ColumnPK[] {new ColumnPK("c6", 1)},
            ""));
    return spSchema;
  }

  private static Map<String, NameAndCols> getToSpanner() {
    Map<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    Map<String, String> t1Cols = new HashMap<String, String>();
    t1Cols.put("product_id", "new_product_id");
    t1Cols.put("quantity", "new_quantity");
    t1Cols.put("user_id", "new_user_id");
    toSpanner.put("cart", new NameAndCols("new_cart", t1Cols));
    Map<String, String> t2Cols = new HashMap<String, String>();
    t2Cols.put("name", "new_name");
    toSpanner.put("people", new NameAndCols("new_people", t2Cols));
    return toSpanner;
  }

  private static Map<String, NameAndCols> getSrcToId() {
    Map<String, NameAndCols> srcToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("product_id", "c1");
    t1ColIds.put("quantity", "c2");
    t1ColIds.put("user_id", "c3");
    srcToId.put("cart", new NameAndCols("t1", t1ColIds));
    Map<String, String> t2ColIds = new HashMap<String, String>();
    t2ColIds.put("name", "c5");
    srcToId.put("people", new NameAndCols("t2", t2ColIds));
    return srcToId;
  }
}
