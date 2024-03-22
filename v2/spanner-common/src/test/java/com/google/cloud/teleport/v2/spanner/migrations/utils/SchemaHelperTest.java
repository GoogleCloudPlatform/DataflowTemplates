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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class SchemaHelperTest {
    private Schema schema;

    @Before
    public void setUp() {
        this.schema = getSchemaObject();
        schema.generateMappings();
    }

    @Test
    public void verifyTableInSessionTestCorrect() {
        try {
            SchemaHelper.verifyTableInSession(schema, "cart");
        } catch (Exception e) {
            fail("No exception should have been thrown for this case");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyTableInSessionTestMissingSrcTable() throws Exception {
        SchemaHelper.verifyTableInSession(schema, "abc");
    }

    @Test(expected = DroppedTableException.class)
    public void verifyTableInSessionTestDroppedTable() throws Exception {
        SchemaHelper.verifyTableInSession(schema, "droppedTableName");
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyTableInSessionTestMissingSpTable() throws Exception {
        Schema schema = getSchemaObject();
        schema.generateMappings();

        // Manually delete key to create invalid session file scenario.
        schema.getSpSchema().remove("t2");
        SchemaHelper.verifyTableInSession(schema, "people");
    }

    private static Schema getSchemaObject() {
        // Add Synthetic PKs.
        Map<String, SyntheticPKey> syntheticPKeys = getSyntheticPks();
        // Add SrcSchema.
        Map<String, SourceTable> srcSchema = getSampleSrcSchema();
        // Add SpSchema.
        Map<String, SpannerTable> spSchema = getSampleSpSchema();
        Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
        expectedSchema.setToSpanner(new HashMap<String, NameAndCols>());
        expectedSchema.setToSource(new HashMap<String, NameAndCols>());
        expectedSchema.setSrcToID(new HashMap<String, NameAndCols>());
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
                        "product_id", new SourceColumnType("varchar", new Long[]{20L}, null)));
        t1SrcColDefs.put(
                "c2", new SourceColumnDefinition("quantity", new SourceColumnType("bigint", null, null)));
        t1SrcColDefs.put(
                "c3",
                new SourceColumnDefinition(
                        "user_id", new SourceColumnType("varchar", new Long[]{20L}, null)));
        srcSchema.put(
                "t1",
                new SourceTable(
                        "cart",
                        "my_schema",
                        new String[]{"c3", "c1", "c2"},
                        t1SrcColDefs,
                        new ColumnPK[]{new ColumnPK("c3", 1), new ColumnPK("c1", 2)}));
        Map<String, SourceColumnDefinition> t2SrcColDefs =
                new HashMap<String, SourceColumnDefinition>();
        t2SrcColDefs.put(
                "c5",
                new SourceColumnDefinition(
                        "name", new SourceColumnType("varchar", new Long[]{20L}, null)));
        srcSchema.put(
                "t2", new SourceTable("people", "my_schema", new String[]{"c5"}, t2SrcColDefs, null));
        // This table is not present in spSchema since this would act as a dropped table.
        Map<String, SourceColumnDefinition> droppedSrcColDefs =
                new HashMap<String, SourceColumnDefinition>();
        droppedSrcColDefs.put(
                "c6",
                new SourceColumnDefinition(
                        "name", new SourceColumnType("varchar", new Long[]{20L}, null)));
        srcSchema.put(
                "t3", new SourceTable("droppedTableName", "my_schema", new String[]{"c6"}, droppedSrcColDefs, null));
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
                        new String[]{"c1", "c2", "c3"},
                        t1SpColDefs,
                        new ColumnPK[]{new ColumnPK("c3", 1), new ColumnPK("c1", 2)},
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
                        new String[]{"c5", "c6"},
                        t2SpColDefs,
                        new ColumnPK[]{new ColumnPK("c6", 1)},
                        ""));
        return spSchema;
    }
}