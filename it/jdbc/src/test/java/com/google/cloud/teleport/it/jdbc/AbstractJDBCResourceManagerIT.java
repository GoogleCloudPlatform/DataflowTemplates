/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.jdbc;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for JDBC Resource Managers. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class AbstractJDBCResourceManagerIT {

  private static final String TEST_ID = "dummy-test";
  private static final String TABLE_NAME = "dummy_table";

  @Test
  public void testDefaultMySQLResourceManagerE2E() {
    DefaultMySQLResourceManager mySQL = DefaultMySQLResourceManager.builder(TEST_ID).build();

    simpleTest(mySQL);
  }

  @Test
  public void testDefaultPostgresResourceManagerE2E() {
    DefaultPostgresResourceManager postgres =
        DefaultPostgresResourceManager.builder(TEST_ID).build();

    simpleTest(postgres);
  }

  @Test
  public void testDefaultOracleResourceManagerE2E() {
    if (System.getProperty("testOnM1") == null) {
      DefaultOracleResourceManager oracle = DefaultOracleResourceManager.builder(TEST_ID).build();
      simpleTest(oracle);
    }
  }

  @Test
  public void testDefaultMSSQLResourceManagerE2E() {
    DefaultMSSQLResourceManager.Builder mssqlBuilder = DefaultMSSQLResourceManager.builder(TEST_ID);
    if (System.getProperty("testOnM1") != null) {
      mssqlBuilder
          .setContainerImageName("mcr.microsoft.com/azure-sql-edge")
          .setContainerImageTag("1.0.6");
    }

    simpleTest(mssqlBuilder.build());
  }

  private <T extends AbstractJDBCResourceManager<?>> void simpleTest(T rm) {
    try {
      HashMap<String, String> columns = new HashMap<>();
      columns.put("first", "VARCHAR(32)");
      columns.put("last", "VARCHAR(32)");
      columns.put("age", "VARCHAR(32)");
      JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns);
      rm.createTable(TABLE_NAME, schema);

      Map<Integer, List<Object>> rows = new HashMap<>();
      rows.put(0, ImmutableList.of("John", "Doe", 23));
      rows.put(1, ImmutableList.of("Jane", "Doe", 42L));
      rows.put(2, List.of('A', 'B', 1));
      rm.write(TABLE_NAME, rows, ImmutableList.of());

      Map<Integer, List<String>> fetchRows = rm.readTable(TABLE_NAME);
      List<String> validateSchema = new ArrayList<>(columns.keySet());
      validateSchema.add(schema.getIdColumn());

      assertThat(rm.getTableSchema(TABLE_NAME)).containsExactlyElementsIn(validateSchema);
      assertThat(fetchRows).hasSize(3);
      assertThat(fetchRows)
          .containsExactlyEntriesIn(
              Map.of(
                  0,
                  ImmutableList.of("John", "Doe", "23"),
                  1,
                  ImmutableList.of("Jane", "Doe", "42"),
                  2,
                  ImmutableList.of("A", "B", "1")));
    } finally {
      rm.cleanupAll();
    }
  }
}
