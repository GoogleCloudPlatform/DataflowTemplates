/*
 * Copyright (C) 2026 Google LLC
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
package com.custom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class CustomTransformationWithShardForBulkITTest {

  @Test
  public void testToSpannerRow_Customers() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("first_name", "John");
    requestRow.put("last_name", "Doe");
    requestRow.put("id", 1L);
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("Customers", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertEquals("John Doe", response.getResponseRow().get("full_name"));
    assertEquals("shard1_1", response.getResponseRow().get("migration_shard_id"));
    assertFalse(response.isEventFiltered());
  }

  @Test
  public void testToSpannerRow_AllDatatypeTransformation_FilterEvent() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("varchar_column", "example1");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest(
            "AllDatatypeTransformation", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertTrue(response.isEventFiltered());
    assertNull(response.getResponseRow());
  }

  @Test
  public void testToSpannerRow_AllDatatypeTransformation_UpdateInsert() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("varchar_column", "example2");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest(
            "AllDatatypeTransformation", requestRow, "shard1", "UPDATE-INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertFalse(response.isEventFiltered());
    assertNull(response.getResponseRow());
  }

  @Test
  public void testToSpannerRow_AllDatatypeTransformation_Normal() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("varchar_column", "example2");
    requestRow.put("tinyint_column", 1L);
    requestRow.put("text_column", "text");
    requestRow.put("int_column", 2L);
    requestRow.put("bigint_column", 3L);
    requestRow.put("float_column", 4.0);
    requestRow.put("double_column", 5.0);
    requestRow.put("decimal_column", "6.0");
    requestRow.put("bool_column", 0L);
    requestRow.put("enum_column", "0");
    requestRow.put("blob_column", "blob");
    requestRow.put("binary_column", "bin");
    requestRow.put("bit_column", 12L);
    requestRow.put("year_column", 2020L);
    requestRow.put("date_column", "2020-01-01");
    requestRow.put("datetime_column", "2020-01-01T12:00:00Z");
    requestRow.put("timestamp_column", "2020-01-01T12:00:00Z");
    requestRow.put("time_column", "12:00:00");

    MigrationTransformationRequest request =
        new MigrationTransformationRequest(
            "AllDatatypeTransformation", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertEquals(2L, response.getResponseRow().get("tinyint_column"));
    assertEquals("text append", response.getResponseRow().get("text_column"));
    assertEquals(3L, response.getResponseRow().get("int_column"));
    assertEquals(4L, response.getResponseRow().get("bigint_column"));
    assertEquals(5.0, (Double) response.getResponseRow().get("float_column"), 0.001);
    assertEquals(6.0, (Double) response.getResponseRow().get("double_column"), 0.001);
    assertEquals("7.0", response.getResponseRow().get("decimal_column"));
    assertEquals(1, response.getResponseRow().get("bool_column"));
    assertEquals("1", response.getResponseRow().get("enum_column"));
    assertEquals("576f726d64", response.getResponseRow().get("blob_column"));
    assertEquals("2020-01-02", response.getResponseRow().get("date_column"));
    assertEquals("2020-01-01T11:59:59Z", response.getResponseRow().get("datetime_column"));
    assertEquals("2020-01-01T11:59:59Z", response.getResponseRow().get("timestamp_column"));
    assertEquals("13:00:00", response.getResponseRow().get("time_column"));
    assertEquals(
        "0102030405060708090A0B0C0D0E0F1011121314", response.getResponseRow().get("binary_column"));
    assertEquals(13, response.getResponseRow().get("bit_column"));
    assertEquals(2021L, response.getResponseRow().get("year_column"));
  }

  @Test
  public void testToSpannerRow_AllDatatypeTransformation_OptionalColumns() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("varchar_column", "example2");
    requestRow.put("varbinary_column", "val");
    requestRow.put("char_column", "val");
    requestRow.put("longblob_column", "val");
    requestRow.put("longtext_column", "val");
    requestRow.put("mediumblob_column", "val");
    requestRow.put("mediumint_column", 1L);
    requestRow.put("mediumtext_column", "val");
    requestRow.put("set_column", "val");
    requestRow.put("smallint_column", 1L);
    requestRow.put("tinyblob_column", "val");
    requestRow.put("tinytext_column", "val");
    requestRow.put("json_column", "val");

    requestRow.put("tinyint_column", 1L);
    requestRow.put("text_column", "text");
    requestRow.put("int_column", 2L);
    requestRow.put("bigint_column", 3L);
    requestRow.put("float_column", 4.0);
    requestRow.put("double_column", 5.0);
    requestRow.put("decimal_column", "6.0");
    requestRow.put("year_column", 2020L);
    requestRow.put("date_column", "2020-01-01");
    requestRow.put("datetime_column", "2020-01-01T12:00:00Z");
    requestRow.put("timestamp_column", "2020-01-01T12:00:00Z");
    requestRow.put("time_column", "12:00:00");

    MigrationTransformationRequest request =
        new MigrationTransformationRequest(
            "AllDatatypeTransformation", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertEquals(
        "0102030405060708090A0B0C0D0E0F1011121314",
        response.getResponseRow().get("varbinary_column"));
    assertEquals("newchar", response.getResponseRow().get("char_column"));
    assertEquals("576f726d64", response.getResponseRow().get("longblob_column"));
    assertEquals("val append", response.getResponseRow().get("longtext_column"));
    assertEquals("576f726d64", response.getResponseRow().get("mediumblob_column"));
    assertEquals(2L, response.getResponseRow().get("mediumint_column"));
    assertEquals("val append", response.getResponseRow().get("mediumtext_column"));
    assertEquals("v3", response.getResponseRow().get("set_column"));
    assertEquals(2L, response.getResponseRow().get("smallint_column"));
    assertEquals("576f726d64", response.getResponseRow().get("tinyblob_column"));
    assertEquals("val append", response.getResponseRow().get("tinytext_column"));
    assertEquals("{\"k1\": \"v1\", \"k2\": \"v2\"}", response.getResponseRow().get("json_column"));
  }

  @Test
  public void testToSpannerRow_UnknownTable() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();

    MigrationTransformationRequest request =
        new MigrationTransformationRequest("Unknown", new HashMap<>(), "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertNull(response.getResponseRow());
    assertFalse(response.isEventFiltered());
  }

  @Test
  public void testToSourceRow() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();
    MigrationTransformationResponse response = transformer.toSourceRow(null);
    assertNotNull(response);
    assertNull(response.getResponseRow());
    assertFalse(response.isEventFiltered());
  }

  @Test
  public void testTransformFailedSpannerMutation() throws Exception {
    CustomTransformationWithShardForBulkIT transformer =
        new CustomTransformationWithShardForBulkIT();
    MigrationTransformationResponse response = transformer.transformFailedSpannerMutation(null);
    assertNotNull(response);
    assertNull(response.getResponseRow());
    assertFalse(response.isEventFiltered());
  }
}
