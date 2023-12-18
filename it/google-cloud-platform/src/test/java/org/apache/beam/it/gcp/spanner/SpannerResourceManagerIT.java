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
package org.apache.beam.it.gcp.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.GCPBaseIT;
import org.apache.beam.it.gcp.GoogleCloudIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(GoogleCloudIntegrationTest.class)
@RunWith(JUnit4.class)
public class SpannerResourceManagerIT extends GCPBaseIT {

  private static final String TABLE_ID = "TestTable";

  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    spannerResourceManager =
        SpannerResourceManager.builder("dummy", PROJECT, REGION)
            .setCredentials(credentials)
            .build();
  }

  @Test
  public void testResourceManagerE2E() {
    // Arrange
    spannerResourceManager.executeDdlStatement(
        "CREATE TABLE "
            + TABLE_ID
            + " ("
            + "RowId INT64 NOT NULL,"
            + "FirstName STRING(1024),"
            + "LastName STRING(1024),"
            + "Company STRING(1024)"
            + ") PRIMARY KEY (RowId)");

    List<Mutation> mutations =
        List.of(
            Mutation.newInsertBuilder(TABLE_ID)
                .set("RowId")
                .to(1)
                .set("FirstName")
                .to("John")
                .set("LastName")
                .to("Doe")
                .set("Company")
                .to("Google")
                .build(),
            Mutation.newInsertBuilder(TABLE_ID)
                .set("RowId")
                .to(2)
                .set("FirstName")
                .to("Jane")
                .set("LastName")
                .to("Doe")
                .set("Company")
                .to("Alphabet")
                .build());

    // Act
    spannerResourceManager.write(mutations);
    long rowCount = spannerResourceManager.getRowCount(TABLE_ID);

    List<Struct> fetchRecords =
        spannerResourceManager.readTableRecords(
            TABLE_ID, List.of("RowId", "FirstName", "LastName", "Company"));

    // Assert
    assertThat(rowCount).isEqualTo(mutations.size());
    assertThat(fetchRecords).hasSize(mutations.size());
    assertThatStructs(fetchRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of("RowId", 1, "FirstName", "John", "LastName", "Doe", "Company", "Google"),
                Map.of("RowId", 2, "FirstName", "Jane", "LastName", "Doe", "Company", "Alphabet")));
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }
}
