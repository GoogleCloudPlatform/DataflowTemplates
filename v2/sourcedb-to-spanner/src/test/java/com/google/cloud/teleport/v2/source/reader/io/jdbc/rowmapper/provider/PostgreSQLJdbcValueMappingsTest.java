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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider;

import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PostgreSQLJdbcValueMappingsTest {

  @Test
  public void testAllMappedTypesHaveRowSizeEstimate() {
    PostgreSQLJdbcValueMappings mappings = new PostgreSQLJdbcValueMappings();
    for (String typeName : mappings.getMappings().keySet()) {
      SourceColumnType sourceColumnType = new SourceColumnType(typeName, new Long[] {10L}, null);
      int size = mappings.estimateColumnSize(sourceColumnType);
      assertTrue("Row size estimate for type " + typeName + " should be > 0", size > 0);
    }
  }

  @Test
  public void testAllMappedTypesHaveRowSizeEstimateWithoutMods() {
    PostgreSQLJdbcValueMappings mappings = new PostgreSQLJdbcValueMappings();
    for (String typeName : mappings.getMappings().keySet()) {
      SourceColumnType sourceColumnType = new SourceColumnType(typeName, null, null);
      int size = mappings.estimateColumnSize(sourceColumnType);
      assertTrue(
          "Row size estimate for type " + typeName + " without mods should be > 0", size > 0);
    }
  }

  @Test
  public void testUnknownTypeReturnsDefaultSize() {
    PostgreSQLJdbcValueMappings mappings = new PostgreSQLJdbcValueMappings();
    SourceColumnType sourceColumnType =
        new SourceColumnType("UNKNOWN_TYPE", new Long[] {10L}, null);
    int size = mappings.estimateColumnSize(sourceColumnType);
    assertTrue("Row size estimate for unknown type should be 65,535", size == 65_535);
  }
}
