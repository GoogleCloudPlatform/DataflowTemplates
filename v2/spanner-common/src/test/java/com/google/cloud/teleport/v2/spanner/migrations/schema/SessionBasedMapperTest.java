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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.type.Type;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class SessionBasedMapperTest {

    SessionBasedMapper mapper;

    @Before
    public void setup() {
        Ddl ddl =
                Ddl.builder()
                        .createTable("new_cart")
                        .column("new_quantity")
                        .int64()
                        .notNull()
                        .endColumn()
                        .column("new_user_id")
                        .string()
                        .size(10)
                        .endColumn()
                        .primaryKey()
                        .asc("new_user_id")
                        .asc("new_quantity")
                        .end()
                        .endTable()
                        .createTable("new_people")
                        .column("synth_id")
                        .int64()
                        .notNull()
                        .endColumn()
                        .column("new_name")
                        .string()
                        .size(10)
                        .endColumn()
                        .primaryKey()
                        .asc("synth_id")
                        .end()
                        .endTable()
                        .build();
        this.mapper = new SessionBasedMapper(getSchemaObject(), ddl);
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
        expectedSchema.generateMappings();
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
        return srcSchema;
    }

    private static Map<String, SpannerTable> getSampleSpSchema() {
        Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
        Map<String, SpannerColumnDefinition> t1SpColDefs =
                new HashMap<String, SpannerColumnDefinition>();
        t1SpColDefs.put(
                "c2", new SpannerColumnDefinition("new_quantity", new SpannerColumnType("INT64", false)));
        t1SpColDefs.put(
                "c3", new SpannerColumnDefinition("new_user_id", new SpannerColumnType("STRING", false)));
        spSchema.put(
                "t1",
                new SpannerTable(
                        "new_cart",
                        new String[]{"c2", "c3"},
                        t1SpColDefs,
                        new ColumnPK[]{new ColumnPK("c3", 1), new ColumnPK("c2", 2)},
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

    @Test
    public void testGetSpannerTableName() {
        String srcTableName = "cart";
        String result = mapper.getSpannerTableName(srcTableName);
        String expectedTableName = "new_cart";
        assertEquals(expectedTableName, result);

        srcTableName = "people";
        result = mapper.getSpannerTableName(srcTableName);
        expectedTableName = "new_people";
        assertEquals(expectedTableName, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerTableNameMissingTable() {
        String srcTable = "wrongTableName";
        mapper.getSpannerTableName(srcTable);
    }

    @Test
    public void testGetSpannerColumnName() {
        String srcTable = "cart";
        String srcColumn = "user_id";
        String result = mapper.getSpannerColumnName(srcTable, srcColumn);
        String expectedColumn = "new_user_id";
        assertEquals(expectedColumn, result);

        srcTable = "cart";
        srcColumn = "quantity";
        result = mapper.getSpannerColumnName(srcTable, srcColumn);
        expectedColumn = "new_quantity";
        assertEquals(expectedColumn, result);


        srcTable = "people";
        srcColumn = "name";
        result = mapper.getSpannerColumnName(srcTable, srcColumn);
        expectedColumn = "new_name";
        assertEquals(expectedColumn, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerColumnNameMissingTable() {
        String srcTable = "wrongTableName";
        String srcColumn = "user_id";
        mapper.getSpannerColumnName(srcTable, srcColumn);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerColumnNameMissingColumn() {
        String srcTable = "cart";
        String srcColumn = "wrongColumn";
        mapper.getSpannerColumnName(srcTable, srcColumn);
    }

    @Test
    public void testGetSourceColumnName() {
        String spannerTable = "new_cart";
        String spannerColumn = "new_quantity";
        String result = mapper.getSourceColumnName(spannerTable, spannerColumn);
        String srcColumn = "quantity";
        assertEquals(srcColumn, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSourceColumnNameMissingTable() {
        String spannerTable = "wrongTableName";
        String spannerColumn = "new_quantity";
        mapper.getSourceColumnName(spannerTable, spannerColumn);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSourceColumnNameMissingColumn() {
        String spannerTable = "new_cart";
        String spannerColumn = "wrongColumn";
        mapper.getSourceColumnName(spannerTable, spannerColumn);
    }

    @Test
    public void testGetSpannerColumnType() {
        String spannerTable = "new_cart";
        String spannerColumn;

        spannerColumn = "new_quantity";
        Type expectedType = Type.int64();
        Type result = mapper.getSpannerColumnType(spannerTable, spannerColumn);
        assertEquals(expectedType, result);

        spannerColumn = "new_user_id";
        expectedType = Type.string();
        result = mapper.getSpannerColumnType(spannerTable, spannerColumn);
        assertEquals(expectedType, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerColumnTypeMissingTable() {
        String spannerTable = "wrongTableName";
        String spannerColumn = "new_quantity";
        mapper.getSpannerColumnType(spannerTable, spannerColumn);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerColumnTypeMissingColumn() {
        String spannerTable = "new_cart";
        String spannerColumn = "wrongColumn";
        mapper.getSpannerColumnType(spannerTable, spannerColumn);
    }

    @Test
    public void testGetSpannerColumns() {
        String spannerTable = "new_cart";
        List<String> expectedColumns = Arrays.asList("new_quantity", "new_user_id");
        List<String> result = mapper.getSpannerColumns(spannerTable);
        Collections.sort(result);
        Collections.sort(expectedColumns);
        assertEquals(expectedColumns, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSpannerColumnsMissingTable() {
        String spannerTable = "wrongTableName";
        mapper.getSpannerColumns(spannerTable);
    }
}
