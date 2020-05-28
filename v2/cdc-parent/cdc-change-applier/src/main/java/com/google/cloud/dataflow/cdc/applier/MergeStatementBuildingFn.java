/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.applier;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds merge statements for synchronizing a table containing a log of changes with a table
 * containing a replica of a source table.
 *
 * The changelog and replica table are joined on a primary key, which is a subset of columns from
 * the replica table. Only the latest change (by timestamp) in the changelog table is utilized to
 * perform updates in the replica table.
 */
public class MergeStatementBuildingFn
    extends DoFn<KV<String, KV<Schema, Schema>>, KV<String, BigQueryAction>> {

  private static final Logger LOG = LoggerFactory.getLogger(MergeStatementBuildingFn.class);

  final String changelogDatasetId;
  final String replicaDatasetId;
  final String projectId;

  private final Counter tablesCreated;
  private final Counter mergeStatementsIssued;

  @StateId("table_created")
  private final StateSpec<ValueState<Boolean>> createdSpec = StateSpecs.value();

  private boolean createdCache;

  MergeStatementBuildingFn(
      String changelogDatasetId,
      String replicaDatasetId,
      String projectId) {
    this.changelogDatasetId = changelogDatasetId;
    this.replicaDatasetId = replicaDatasetId;
    this.projectId = projectId;
    this.createdCache = false;

    this.tablesCreated = Metrics.counter(
        MergeStatementBuildingFn.class, "createTableStatementsIssued");
    this.mergeStatementsIssued = Metrics.counter(
        MergeStatementBuildingFn.class, "mergeStatementsIssued");
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("table_created") ValueState<Boolean> tableCreated) {
    KV<String, KV<Schema, Schema>> tableAndSchemas = c.element();

    // Start by actually fetching whether we created the table or not.
    if (!createdCache) {
      Boolean actuallyCreated = firstNonNull(tableCreated.read(), false);
      createdCache = actuallyCreated;
    }
    // Once we know for sure if we created the table, then we act on creating (or not).
    if (!createdCache) {
      tableCreated.write(true);
      createdCache = true;
      c.output(KV.of(tableAndSchemas.getKey(),
          buildCreateTableAction(tableAndSchemas, projectId, replicaDatasetId)));
      this.tablesCreated.inc();
    }

    c.output(KV.of(tableAndSchemas.getKey(),
        buildMergeStatementAction(
            tableAndSchemas, projectId, changelogDatasetId, replicaDatasetId)));
    this.mergeStatementsIssued.inc();
  }

  static final BigQueryAction buildCreateTableAction(
      KV<String, KV<Schema, Schema>> tableAndSchemas,
      String projectId,
      String replicaDatasetId) {
    Schema tableSchema = tableAndSchemas.getValue().getValue();
    String tableName = ChangelogTableDynamicDestinations.getBigQueryTableName(
        tableAndSchemas.getKey(), false);
    return BigQueryAction.createTable(
        projectId, replicaDatasetId, tableName, tableSchema);
  }

  static final BigQueryAction buildMergeStatementAction(
      KV<String, KV<Schema, Schema>> tableAndSchemas,
      String projectId,
      String changelogDatasetId,
      String replicaDatasetId) {
    String tableName = ChangelogTableDynamicDestinations.getBigQueryTableName(
        tableAndSchemas.getKey(), false);

    String changeLogTableName = ChangelogTableDynamicDestinations.getBigQueryTableName(
        tableAndSchemas.getKey(), true);

    Schema primaryKeySchema = tableAndSchemas.getValue().getKey();
    List<String> pkColumnNames = primaryKeySchema.getFields().stream()
        .map(f -> f.getName()).collect(Collectors.toList());

    Schema allColumnsSchema = tableAndSchemas.getValue().getValue();
    List<String> allColumnNames = allColumnsSchema.getFields().stream()
        .map(f -> f.getName()).collect(Collectors.toList());

    String mergeStatement = buildQueryMergeReplicaTableWithChangeLogTable(
        tableName, changeLogTableName,
        pkColumnNames, allColumnNames,
        projectId, changelogDatasetId, replicaDatasetId);
    LOG.debug("Merge statement for table {} is: {}", tableName, mergeStatement);
    return BigQueryAction.query(mergeStatement);
  }

  static final String CREATE_TABLE_TEMPLATE = String.join("",
      "CREATE TABLE IF NOT EXISTS `%s.%s.%s` ");

  static final String MERGE_REPLICA_WITH_CHANGELOG_TABLES = String.join("",
      "MERGE `%s.%s.%s` AS replica ",
      "USING (%s) AS changelog ",
      "ON %s ",
      "WHEN MATCHED AND changelog.operation = \"DELETE\" ",
      "THEN DELETE ",
      "WHEN MATCHED AND changelog.operation = \"UPDATE\" THEN %s ",
      "WHEN NOT MATCHED BY TARGET AND changelog.operation = \"INSERT\" THEN %s");

  public static String buildQueryMergeReplicaTableWithChangeLogTable(
      String replicaTableName, String changelogTableName,
      List<String>pkColumns, List<String> rowColumns,
      String projectId, String changelogDatasetId, String replicaDatasetId) {

    String changeLogLatestChangePerKey = buildQueryGetLatestChangePerPrimaryKey(
        changelogTableName, pkColumns, projectId, changelogDatasetId);

    String finalMergeQuery = String.format(MERGE_REPLICA_WITH_CHANGELOG_TABLES,
        projectId, replicaDatasetId, replicaTableName,   // Target table for merge
        changeLogLatestChangePerKey,                        // Source table for merge
        buildJoinConditions(
            pkColumns, "replica", "changelog" + ".primaryKey"),
        buildUpdateStatement(rowColumns),                   // Perform update
        buildInsertStatement(rowColumns,
            rowColumns.stream().map(col -> "fullRecord." + col).collect(Collectors.toList())));
    return finalMergeQuery;
  }

  static final String UPDATE_STATEMENT = "UPDATE SET %s";

  static String buildUpdateStatement(List<String> rowColumns) {
    List<String> assignmentStatements = rowColumns.stream()
        .map(column -> String.format("%s = changelog.fullRecord.%s", column, column))
        .collect(Collectors.toList());

    return String.format(UPDATE_STATEMENT,
        String.join(", ", assignmentStatements));
  }

  static final String INSERT_STATEMENT = "INSERT(%s) VALUES (%s)";

  static String buildInsertStatement(
      List<String> primaryRowColumns, List<String> secondaryRowColumns) {
    return String.format(INSERT_STATEMENT,
        String.join(", ", primaryRowColumns),
        String.join(", ", secondaryRowColumns));
  }

  static final String LATEST_CHANGE_PER_PK = String.join("",
      "SELECT * ",
      "FROM (%s) AS ts_table ",
      "INNER JOIN `%s.%s.%s` AS source_table ",
      "ON %s"
  );

  static final String MAXIMUM_TIMESTAMP_PER_PK =
      "SELECT %s, MAX(%s) as max_ts_ms FROM `%s.%s.%s` GROUP BY %s";

  public static String buildQueryGetLatestChangePerPrimaryKey(
      String tableName, List<String>pkColumns, String projectId, String datasetId) {
    List<String> qualifiedPkColumns = pkColumns.
        stream()
        .map(s -> String.format("primaryKey.%s", s))
        .collect(Collectors.toList());

    String maxTimestampPerPkStatement = buildQueryGetMaximumTimestampPerPrimaryKey(
        tableName, qualifiedPkColumns, projectId, datasetId);

    String joinStatement = buildJoinConditions(
        pkColumns, "source_table.primaryKey", "ts_table")
        + " AND source_table.timestampMs = ts_table.max_ts_ms";

    String fullStatement = String.format(
        LATEST_CHANGE_PER_PK,
        maxTimestampPerPkStatement,       // FROM PARAMETERS
        projectId, datasetId, tableName,  // INNER JOIN PARAMETERS
        joinStatement);                   // JOIN STATEMENT (e.g. ON A.a = B.a AND A.c = B.c ....)

    return fullStatement;
  }

  public static String buildJoinConditions(
      List<String> pkColumns, final String leftTableName, final String rightTableName) {
    List<String> equalityConditions = pkColumns.stream()
        .map(col -> String.format("%s.%s = %s.%s", leftTableName, col, rightTableName, col))
        .collect(Collectors.toList());
    return String.join(" AND ", equalityConditions);
  }

  public static String buildQueryGetMaximumTimestampPerPrimaryKey(
      String tableName, List<String>pkColumns, String projectId, String datasetId) {
    String pkCommaJoinedString = String.join(", ", pkColumns);

    String maxTimestampPerPkStatement = String.format(
        MAXIMUM_TIMESTAMP_PER_PK,
        pkCommaJoinedString, "timestampMs",  // SELECT SECTION PARAMETERS
        projectId, datasetId, tableName,                // FROM SECTION PARAMETERS
        pkCommaJoinedString);                           // GROUP BY SECTION PARAMETERS
    return maxTimestampPerPkStatement;
  }

}