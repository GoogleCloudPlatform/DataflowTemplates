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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class CustomTransformationForDLQITTest {

  private CustomTransformationForDLQIT transformer;

  @Before
  public void setUp() {
    transformer = new CustomTransformationForDLQIT();
  }

  @Test
  public void testInit() {
    transformer.init("mode=semi-fixed");
    // Verified via behavior in other tests
  }

  @Test
  public void testToSpannerRow_AllDataTypes_BadMode_CrashingId() {
    transformer.init("mode=bad");

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("id", 999L);
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("AllDataTypes", requestRow, "shard1", "INSERT");

    try {
      transformer.toSpannerRow(request);
      fail("Expected InvalidTransformationException");
    } catch (InvalidTransformationException e) {
      assertEquals("Simulated failure for id 999", e.getMessage());
    }
  }

  @Test
  public void testToSpannerRow_AllDataTypes_BadMode_NonCrashingId() throws Exception {
    transformer.init("mode=bad");

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("id", 888L); // 888 doesn't crash in toSpannerRow even in bad mode!
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("AllDataTypes", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertFalse(response.isEventFiltered());
  }

  @Test
  public void testToSpannerRow_Orders() throws Exception {
    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("OrderSource", "WEB");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("Orders", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertNotNull(response);
    assertEquals("'WEB_v1'", response.getResponseRow().get("LegacyOrderSystem"));
  }

  @Test
  public void testToSourceRow_AllDataTypes_BadMode_CrashingId() {
    transformer.init("mode=bad");

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("id", 888L);
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("AllDataTypes", requestRow, "shard1", "INSERT");

    try {
      transformer.toSourceRow(request);
      fail("Expected InvalidTransformationException");
    } catch (InvalidTransformationException e) {
      assertEquals("Simulated failure for id 888", e.getMessage());
    }
  }

  @Test
  public void testToSourceRow_AllDataTypes_SemiFixedMode_CrashingId() {
    transformer.init("mode=semi-fixed");

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("id", 888L);
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("AllDataTypes", requestRow, "shard1", "INSERT");

    try {
      transformer.toSourceRow(request);
      fail("Expected InvalidTransformationException");
    } catch (InvalidTransformationException e) {
      assertEquals("Simulated failure for id 888", e.getMessage());
    }
  }

  @Test
  public void testToSourceRow_AllDataTypes_SemiFixedMode_NonCrashingId() throws Exception {
    transformer.init("mode=semi-fixed");

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("id", 999L); // 999 doesn't crash in semi-fixed mode in toSourceRow!
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("AllDataTypes", requestRow, "shard1", "INSERT");

    MigrationTransformationResponse response = transformer.toSourceRow(request);
    assertNotNull(response);
  }

  @Test
  public void testTransformFailedSpannerMutation() throws Exception {
    MigrationTransformationResponse response = transformer.transformFailedSpannerMutation(null);
    assertNotNull(response);
    assertTrue(response.getResponseRow().isEmpty());
  }
}
