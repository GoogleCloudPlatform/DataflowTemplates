/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.parquet.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for JDBC Resource Managers. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class CloudSqlResourceManagerIT {

  private static final Logger LOG = LoggerFactory.getLogger(CloudSqlResourceManagerIT.class);

  private static final String TEST_ID = "dummy-test";
  private static final String TABLE_NAME = "dummy_table";

  @Test
  public void testDefaultCloudMySQLResourceManagerE2E() {
    if (missingProperties("cloudProxyHost", "cloudProxyPort")) {
      return;
    }
    CloudMySQLResourceManager mySQL = CloudMySQLResourceManager.builder(TEST_ID).build();

    simpleTest(mySQL);
  }

  @Test
  public void testDefaultCloudOracleResourceManagerE2E() {
    if (missingProperties("cloudOracleHost")) {
      return;
    }
    CloudOracleResourceManager oracle = CloudOracleResourceManager.builder(TEST_ID).build();

    simpleTest(oracle);
  }

  private <T extends CloudSqlResourceManager> void simpleTest(T rm) {
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

  private boolean missingProperties(String... properties) {
    for (String property : properties) {
      if (Strings.isNullOrEmpty(System.getProperty(property))) {
        LOG.info(String.format("-D%s was not specified, skipping...", property));
        return true;
      }
    }
    return false;
  }
}
