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
package com.custom;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Tests for CustomTransformationFetcher class. */
public class CustomTransformationFetcherTest {
  @Test
  public void endToEndTest() throws InvalidTransformationException {
    CustomTransformationFetcher customTransformationFetcher = new CustomTransformationFetcher();
    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("first_name", "abc");
    requestRow.put("last_name", "xyz");
    requestRow.put("id", "123");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("Customers", requestRow, "ls1", "");
    MigrationTransformationResponse response = customTransformationFetcher.toSpannerRow(request);
    MigrationTransformationResponse response2 =
        customTransformationFetcher.toSourceRow(
            new MigrationTransformationRequest("Customers", response.getResponseRow(), "ls1", ""));
    assertEquals(request.getRequestRow(), response2.getResponseRow());
  }

  @Test
  public void testToSourceRowInvalidTableName() throws InvalidTransformationException {
    CustomTransformationFetcher customTransformationFetcher = new CustomTransformationFetcher();
    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("first_name", "abc");
    requestRow.put("last_name", "xyz");
    requestRow.put("id", "123");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("xyz", requestRow, "ls1", "");
    MigrationTransformationResponse response = customTransformationFetcher.toSourceRow(request);
    assertEquals(response.getResponseRow(), null);
  }

  @Test
  public void testToSpannerRowInvalidTableName() throws InvalidTransformationException {
    CustomTransformationFetcher customTransformationFetcher = new CustomTransformationFetcher();
    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("first_name", "abc");
    requestRow.put("last_name", "xyz");
    requestRow.put("id", "123");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("xyz", requestRow, "ls1", "");
    MigrationTransformationResponse response = customTransformationFetcher.toSpannerRow(request);
    assertEquals(response.getResponseRow(), null);
  }
}
