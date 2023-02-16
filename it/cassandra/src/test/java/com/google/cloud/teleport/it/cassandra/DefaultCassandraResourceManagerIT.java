/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.cassandra;

import static com.google.cloud.teleport.it.gcp.matchers.TemplateAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertThat;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link DefaultCassandraResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class DefaultCassandraResourceManagerIT {

  private DefaultCassandraResourceManager cassandraResourceManager;

  @Before
  public void setUp() throws IOException {
    cassandraResourceManager = DefaultCassandraResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(Map.of("id", 1, "company", "Google"));
    records.add(Map.of("id", 2, "company", "Alphabet"));

    cassandraResourceManager.executeStatement(
        "CREATE TABLE dummy_insert ( id int PRIMARY KEY, company text )");

    boolean insertDocuments = cassandraResourceManager.insertDocuments("dummy_insert", records);
    assertThat(insertDocuments).isTrue();

    Iterable<Row> fetchRecords = cassandraResourceManager.readTable("dummy_insert");
    List<Map<String, Object>> convertedRecords =
        CassandraTestUtils.cassandraRowsToRecords(fetchRecords);
    assertThatRecords(convertedRecords).hasRows(2);
    assertThatRecords(convertedRecords)
        .hasRecordsUnordered(
            List.of(Map.of("id", 1, "company", "Google"), Map.of("id", 2, "company", "Alphabet")));
  }

  @After
  public void tearDown() {
    if (cassandraResourceManager != null) {
      cassandraResourceManager.cleanupAll();
    }
  }
}
