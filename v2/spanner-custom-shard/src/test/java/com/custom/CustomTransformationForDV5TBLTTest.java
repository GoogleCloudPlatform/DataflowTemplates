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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class CustomTransformationForDV5TBLTTest {

  @Test
  public void init() {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();
    transformer.init("params");
  }

  @Test
  public void toSpannerRow_matchingShard() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("col1", "val1");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("table1", requestRow, "shard_1", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertThat(response).isNotNull();
    assertThat(response.getResponseRow()).isNull();
    assertThat(response.isEventFiltered()).isFalse();
  }

  @Test
  public void toSpannerRow_mismatchedShard() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("col1", "val1");
    requestRow.put("col2", "val2");

    MigrationTransformationResponse table1Response =
        transformer.toSpannerRow(
            new MigrationTransformationRequest("table1", requestRow, "shard_2", "INSERT"));
    assertThat(table1Response).isNotNull();
    assertThat(table1Response.getResponseRow()).isEqualTo(Map.of("col1", "val1_mutated"));
    assertThat(table1Response.isEventFiltered()).isFalse();

    MigrationTransformationResponse table2Response =
        transformer.toSpannerRow(
            new MigrationTransformationRequest("table2", requestRow, "shard_21", "INSERT"));
    assertThat(table2Response).isNotNull();
    assertThat(table2Response.getResponseRow()).isEqualTo(Map.of("col1", "val1_mutated"));
    assertThat(table2Response.isEventFiltered()).isFalse();
  }

  @Test
  public void toSpannerRow_nullShardId() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();

    Map<String, Object> requestRow = new HashMap<>();
    requestRow.put("col1", "val1");
    MigrationTransformationRequest request =
        new MigrationTransformationRequest("table1", requestRow, null, "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertThat(response).isNotNull();
    assertThat(response.getResponseRow()).isEqualTo(Map.of("col1", "val1_mutated"));
    assertThat(response.isEventFiltered()).isFalse();
  }

  @Test
  public void toSpannerRow_nullCol1() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();

    MigrationTransformationRequest request =
        new MigrationTransformationRequest("table1", new HashMap<>(), "shard_2", "INSERT");

    MigrationTransformationResponse response = transformer.toSpannerRow(request);
    assertThat(response).isNotNull();
    assertThat(response.getResponseRow()).isNull();
    assertThat(response.isEventFiltered()).isFalse();
  }

  @Test
  public void toSourceRow() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();
    MigrationTransformationResponse response = transformer.toSourceRow(null);
    assertThat(response).isNotNull();
    assertThat(response.getResponseRow()).isNull();
    assertThat(response.isEventFiltered()).isFalse();
  }

  @Test
  public void transformFailedSpannerMutation() throws Exception {
    CustomTransformationForDV5TBLT transformer = new CustomTransformationForDV5TBLT();
    MigrationTransformationResponse response = transformer.transformFailedSpannerMutation(null);
    assertThat(response).isNotNull();
    assertThat(response.getResponseRow()).isNull();
    assertThat(response.isEventFiltered()).isFalse();
  }
}
