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
    Map<String, Map<String, String>> renamedColumns = new HashMap<>();
    Map<String, String> tableColumnMap = new HashMap<>();
    tableColumnMap.put("Lorem", "Epsum");
    renamedColumns.put("table1", tableColumnMap);
    schemaFileOverride = new SchemaFileOverride(null, renamedColumns);
    assertEquals(renamedColumns, schemaFileOverride.getRenamedColumnTupleMap());
  }
}
