/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.cdc.merge;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** MergeInfo tests to validate merge is built correctly. */
@RunWith(JUnit4.class)
public final class MergeInfoTest {

  private static final String MERGE_SQL =
      "MERGE `projectId.dataset.table` AS replica USING (SELECT `id,"
          + " cola`,`colb`,`timestamp`,`other` FROM (SELECT `id, cola`,`colb`,`timestamp`,`other`,"
          + " ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC, other DESC,"
          + " metadata_deleteField ASC) as row_num FROM `projectId.dataset.staging_table` WHERE"
          + " COALESCE(_PARTITIONTIME, CURRENT_TIMESTAMP()) >= TIMESTAMP(DATE_ADD(CURRENT_DATE(),"
          + " INTERVAL -2 DAY)) AND (COALESCE(_PARTITIONTIME, CURRENT_TIMESTAMP()) >="
          + " TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))    OR (_PARTITIONTIME >="
          + " TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))        AND"
          + " metadata_deleteField))) WHERE row_num=1) AS staging ON replica.id = staging.id WHEN"
          + " MATCHED AND replica.timestamp <= staging.timestamp AND replica.other <= staging.other"
          + " AND staging.metadata_deleteField=True THEN DELETE WHEN MATCHED AND replica.timestamp"
          + " <= staging.timestamp AND replica.other <= staging.other THEN UPDATE SET `id, cola` ="
          + " staging.id, cola, `colb` = staging.colb, `timestamp` = staging.timestamp, `other` ="
          + " staging.other WHEN NOT MATCHED BY TARGET AND staging.metadata_deleteField!=True THEN"
          + " INSERT(`id, cola`,`colb`,`timestamp`,`other`) VALUES (staging.id, cola, staging.colb,"
          + " staging.timestamp, staging.other)";

  @Test
  public void create_expectedResult() {
    List<String> allPkFields = ImmutableList.of("id");
    List<String> orderByFields = ImmutableList.of("timestamp", "other");
    TableId stagingTable = TableId.of("projectId", "dataset", "staging_table");
    TableId replicaTable = TableId.of("projectId", "dataset", "table");

    MergeInfo mergeInfo =
        MergeInfo.create(
            "projectId",
            allPkFields,
            orderByFields,
            "metadata_deleteField",
            stagingTable,
            replicaTable);

    assertThat(mergeInfo.getCustomColumns()).isEmpty();
    assertThat(mergeInfo.getStagingTable()).isEqualTo(stagingTable);
    assertThat(mergeInfo.getReplicaTable()).isEqualTo(replicaTable);
  }

  @Test
  public void getReplicaTableReference_expectedResult() {
    List<String> allPkFields = ImmutableList.of("id");
    List<String> orderByFields = ImmutableList.of("timestamp", "other");
    TableId stagingTable = TableId.of("projectId", "dataset", "staging_table");
    TableId replicaTable = TableId.of("projectId", "dataset", "table");
    MergeInfo mergeInfo =
        MergeInfo.create(
            "projectId",
            allPkFields,
            orderByFields,
            "metadata_deleteField",
            stagingTable,
            replicaTable);

    assertThat(mergeInfo.getReplicaTableReference()).isEqualTo("projectId.dataset.table");
  }

  @Test
  public void getStagingTableReference_expectedResult() {
    List<String> allPkFields = ImmutableList.of("id");
    List<String> orderByFields = ImmutableList.of("timestamp", "other");
    TableId stagingTable = TableId.of("projectId", "dataset", "staging_table");
    TableId replicaTable = TableId.of("projectId", "dataset", "table");
    MergeInfo mergeInfo =
        MergeInfo.create(
            "projectId",
            allPkFields,
            orderByFields,
            "metadata_deleteField",
            stagingTable,
            replicaTable);

    assertThat(mergeInfo.getStagingTableReference()).isEqualTo("projectId.dataset.staging_table");
  }

  @Test
  public void buildMergeStatement_expectedResult() {
    List<String> allPkFields = ImmutableList.of("id");
    List<String> orderByFields = ImmutableList.of("timestamp", "other");
    List<String> mergeFields = ImmutableList.of("id, cola", "colb", "timestamp", "other");
    TableId stagingTable = TableId.of("projectId", "dataset", "staging_table");
    TableId replicaTable = TableId.of("projectId", "dataset", "table");
    MergeConfiguration cfg = MergeConfiguration.bigQueryConfiguration();
    MergeInfo mergeInfo =
        MergeInfo.create(
            "projectId",
            allPkFields,
            orderByFields,
            "metadata_deleteField",
            stagingTable,
            replicaTable,
            mergeFields);

    assertThat(mergeInfo.buildMergeStatement(cfg)).isEqualTo(MERGE_SQL);
  }
}
