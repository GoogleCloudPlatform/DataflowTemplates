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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** Integration tests for JDBC Resource Managers. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class AbstractJDBCResourceManagerIT {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCResourceManagerIT.class);

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
    // Oracle image does not work on M1
    if (System.getProperty("testOnM1") != null) {
      LOG.info("M1 is being used, Oracle tests are not being executed.");
      return;
    }

    DefaultOracleResourceManager oracle = DefaultOracleResourceManager.builder(TEST_ID).build();
    simpleTest(oracle);
  }

  @Test
  public void testDefaultMSSQLResourceManagerE2E() {
    DefaultMSSQLResourceManager mssqlBuilder = DefaultMSSQLResourceManager.builder(TEST_ID).build();
    simpleTest(mssqlBuilder);
  }

  private <T extends AbstractJDBCResourceManager<?>> void simpleTest(T rm) {
    try {
      Map<String, String> columns = new LinkedHashMap<>();
      columns.put("id", "INTEGER");
      columns.put("first", "VARCHAR(32)");
      columns.put("last", "VARCHAR(32)");
      columns.put("age", "VARCHAR(32)");
      JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
      rm.createTable(TABLE_NAME, schema);

      List<Map<String, Object>> rows = new ArrayList<>();
      rows.add(ImmutableMap.of("id", 0, "first", "John", "last", "Doe", "age", 23));
      rows.add(ImmutableMap.of("id", 1, "first", "Jane", "last", "Doe", "age", 42));
      rows.add(ImmutableMap.of("id", 2, "first", "A", "last", "B", "age", 1));
      rm.write(TABLE_NAME, rows);

      List<String> validateSchema = new ArrayList<>(columns.keySet());
      List<Map<String, Object>> fetchRows = rm.readTable(TABLE_NAME);

      // toUpperCase expected because some databases (Postgres, Oracle) transform column names
      assertThat(toUpperCase(rm.getTableSchema(TABLE_NAME)))
          .containsExactlyElementsIn(toUpperCase(validateSchema));
      assertThat(fetchRows).hasSize(3);
      assertThatRecords(fetchRows).hasRecordsUnorderedCaseInsensitiveColumns(rows);
    } finally {
      rm.cleanupAll();
    }
  }

  private List<String> toUpperCase(List<String> list) {
    return list.stream().map(String::toUpperCase).collect(Collectors.toList());
  }
}
