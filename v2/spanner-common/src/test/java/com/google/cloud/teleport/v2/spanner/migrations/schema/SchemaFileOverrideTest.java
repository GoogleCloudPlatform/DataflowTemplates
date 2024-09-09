package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static junit.framework.TestCase.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SchemaFileOverrideTest {

  SchemaFileOverride schemaFileOverride;

  @Test
  public void testGetRenamedTables() {
    Map<String, String> renamedTables = new HashMap<>();
    renamedTables.put("Lorem", "Epsum");
    schemaFileOverride = new SchemaFileOverride(renamedTables, null);
    assertEquals(renamedTables, schemaFileOverride.getRenamedTables());
  }

  @Test
  public void testGetRenamedColumns() {
    Map<String, Map<String, String>>  renamedColumns = new HashMap<>();
    Map<String, String> tableColumnMap = new HashMap<>();
    tableColumnMap.put("Lorem", "Epsum");
    renamedColumns.put("table1", tableColumnMap);
    schemaFileOverride = new SchemaFileOverride(null, renamedColumns);
    assertEquals(renamedColumns, schemaFileOverride.getRenamedColumns());
  }

}