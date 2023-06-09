/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

/** Tests for MergeStatementBuildingFn. */
public class MergeStatementBuildingFnTest {
  static final String TABLE_ID = "myTable";
  static final String CHANGELOG_TABLE_ID = "myTable_changelog";
  static final String PROJECT_ID = "myProject";
  static final String CHANGELOG_DATASET_ID = "myChangelogDataset";
  static final String REPLICA_DATASET_ID = "myReplicaDataset";

  static final List<String> PRIMARY_KEY_COLUMNS = Arrays.asList("pk1", "pk2");
  static final List<String> FULL_COLUMN_LIST = Arrays.asList("pk1", "pk2", "col1", "col2", "col3");

  @Test
  void testFullMergeStatementIsBuilt() {}

  @Test
  void testGetMaximumTimestampPerPrimaryKey() {
    String getMaximumTsPkQuery =
        MergeStatementBuildingFn.buildQueryGetMaximumTimestampPerPrimaryKey(
            TABLE_ID, PRIMARY_KEY_COLUMNS, PROJECT_ID, CHANGELOG_DATASET_ID);

    assertThat(
        getMaximumTsPkQuery,
        equalTo(
            String.join(
                "",
                "SELECT pk1, pk2, MAX(timestampMs) as max_ts_ms FROM ",
                "`myProject.myChangelogDataset.myTable` GROUP BY pk1, pk2")));
  }

  @Test
  void testGetNewestElementPerPrimaryKey() {
    String getLatestElementPkQuery =
        MergeStatementBuildingFn.buildQueryGetLatestChangePerPrimaryKey(
            TABLE_ID, PRIMARY_KEY_COLUMNS, PROJECT_ID, CHANGELOG_DATASET_ID);

    assertThat(
        getLatestElementPkQuery,
        containsString(
            "(SELECT primaryKey.pk1, primaryKey.pk2, MAX(timestampMs) as max_ts_ms "
                + "FROM `myProject.myChangelogDataset.myTable` "
                + "GROUP BY primaryKey.pk1, primaryKey.pk2) AS ts_table"));

    assertThat(
        getLatestElementPkQuery,
        containsString(
            "ON source_table.primaryKey.pk1 = ts_table.pk1 "
                + "AND source_table.primaryKey.pk2 = ts_table.pk2 "
                + "AND source_table.timestampMs = ts_table.max_ts_ms"));

    assertThat(
        getLatestElementPkQuery,
        containsString("INNER JOIN `myProject.myChangelogDataset.myTable` AS source_table"));
  }

  @Test
  void testFullMergeStatement() {
    String fullMergeStatement =
        MergeStatementBuildingFn.buildQueryMergeReplicaTableWithChangeLogTable(
            TABLE_ID,
            CHANGELOG_TABLE_ID,
            PRIMARY_KEY_COLUMNS,
            FULL_COLUMN_LIST,
            PROJECT_ID,
            CHANGELOG_DATASET_ID,
            REPLICA_DATASET_ID);

    assertThat(
        fullMergeStatement,
        containsString(
            "ON "
                + "replica.pk1 = changelog.primaryKey.pk1 "
                + "AND replica.pk2 = changelog.primaryKey.pk2 "));

    assertThat(
        fullMergeStatement,
        containsString("MERGE `myProject.myReplicaDataset.myTable` AS replica"));

    assertThat(
        fullMergeStatement,
        containsString(
            "WHEN MATCHED THEN UPDATE SET "
                + "pk1 = changelog.fullRecord.pk1, "
                + "pk2 = changelog.fullRecord.pk2, "
                + "col1 = changelog.fullRecord.col1, "
                + "col2 = changelog.fullRecord.col2, "
                + "col3 = changelog.fullRecord.col3 "));

    assertThat(
        fullMergeStatement,
        containsString(
            "WHEN NOT MATCHED BY TARGET AND changelog.operation != \"DELETE\" THEN "
                + "INSERT(pk1, pk2, col1, col2, col3) "
                + "VALUES ("
                + "fullRecord.pk1, fullRecord.pk2, fullRecord.col1, fullRecord.col2, fullRecord.col3)"));
  }

  private static final String TABLE_1_NAME = "project.dataset.myTable1";

  private static final String TABLE_2_NAME = "project.dataset.myTable2";

  private static final Schema TABLE_1_SCHEMA =
      Schema.of(
          Schema.Field.of("team", Schema.FieldType.STRING),
          Schema.Field.nullable("country", Schema.FieldType.STRING),
          Schema.Field.nullable("city", Schema.FieldType.STRING),
          Schema.Field.nullable("year_founded", Schema.FieldType.INT16));

  private static final Schema TABLE_1_PK_SCHEMA =
      Schema.of(Schema.Field.of("team", Schema.FieldType.STRING));

  private static final Schema TABLE_2_SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.nullable("timestamp", Schema.FieldType.DATETIME),
          Schema.Field.nullable("description", Schema.FieldType.STRING));

  private static final Schema TABLE_2_PK_SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.nullable("timestamp", Schema.FieldType.DATETIME));

  @Test
  public void testTablesBuiltInPipeline() {
    Pipeline p = Pipeline.create();

    PCollection<KV<String, KV<Schema, Schema>>> tableSchemaS =
        p.apply(
            Create.of(
                KV.of(TABLE_1_NAME, KV.of(TABLE_1_PK_SCHEMA, TABLE_1_SCHEMA)),
                KV.of(TABLE_2_NAME, KV.of(TABLE_2_PK_SCHEMA, TABLE_2_SCHEMA)),
                KV.of(TABLE_1_NAME, KV.of(TABLE_1_PK_SCHEMA, TABLE_1_SCHEMA))));

    PCollection<KV<String, BigQueryAction>> issuedStatements =
        tableSchemaS.apply(
            ParDo.of(
                new MergeStatementBuildingFn(
                    CHANGELOG_DATASET_ID, REPLICA_DATASET_ID, PROJECT_ID)));

    PCollection<KV<String, Long>> statementsIssued =
        issuedStatements.apply("CountCreateActions", Count.perKey());

    PCollection<KV<String, Long>> tablesCreatedCount =
        issuedStatements
            .apply(
                "GetCreateActions",
                Filter.by(input -> input.getValue().action.equals(BigQueryAction.CREATE_TABLE)))
            .apply("CountCreateActions", Count.perKey());

    PCollection<KV<String, Long>> tablesMerged =
        issuedStatements
            .apply(
                "GetMergeActions",
                Filter.by(input -> input.getValue().action.equals(BigQueryAction.STATEMENT)))
            .apply("CountMergeActions", Count.perKey());

    PAssert.that(statementsIssued)
        .containsInAnyOrder(KV.of(TABLE_1_NAME, 3L), KV.of(TABLE_2_NAME, 2L));

    PAssert.that(tablesCreatedCount)
        .containsInAnyOrder(KV.of(TABLE_1_NAME, 1L), KV.of(TABLE_2_NAME, 1L));

    PAssert.that(tablesMerged).containsInAnyOrder(KV.of(TABLE_1_NAME, 2L), KV.of(TABLE_2_NAME, 1L));

    p.run().waitUntilFinish();
  }
}
