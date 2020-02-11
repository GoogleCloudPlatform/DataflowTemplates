/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.bigquery;

import java.util.List;
import java.util.stream.Collectors;

public class BigQueryMergeBuilder {

  public static final String STAGING_TABLE_NAME = "staging";
  public static final String REPLICA_TABLE_NAME = "replica";

  public static final String DEFAULT_TIMESTAMP_FIELD = "_metadata_timestamp";
  public static final String DEFAULT_DELETED_FIELD = "_metadata_deleted";
  public static final Integer DEFAULT_RETENTION = 3;

  public static String buildMergeStatementWithDefaults(String replicaTable,
      String stagingTable,
      List<String> primaryKeyFields,
      List<String> allFields) {
    return buildMergeStatement(replicaTable, stagingTable, primaryKeyFields, allFields,
        DEFAULT_TIMESTAMP_FIELD, DEFAULT_DELETED_FIELD, DEFAULT_RETENTION);
  }

  public static String buildMergeStatementWithDefaultRetention(
      String replicaTable,
      String stagingTable,
      List<String> primaryKeyFields,
      List<String> allFields,
      String timestampField,
      String deletedField) {
    return buildMergeStatement(replicaTable, stagingTable, primaryKeyFields, allFields,
        timestampField, deletedField, DEFAULT_RETENTION);
  }

  /**
   * The top-level template for building merge statements to be issued to BigQuery.
   *
   * The way to read it is:
   *
   * Merge the REPLICA table
   * Using a view of the STAGING table containing the latest changes
   * On a join condition (join on all the columns of the primary key).
   * If there is a match, and there has been a deletion:
   * -- This means that both STAGING and REPLICA tables contain the primary key, and that the
   *    latest change in that primary key is a deletion. <b>Therefore delete the row.</b>
   * If there is a match, and the STAGING table contains a newer record:
   * -- This means that both STAGING and REPLICA tables contain the primary key, and that
   *    the STAGING table contains a newer version of the data. <b>Therefore, update the row.</b>
   * If there has not been a match:
   * -- This means that the REPLICA table does not contain a row that is contained in the STAGING
   *    table. <b></b>Therefore, insert this new row.</b>
   */
  public static final String MERGE_TEMPLATE = String.join("",
      "MERGE `%s` AS %s ",
      "USING (%s) AS %s ",
      "ON %s ",
      "WHEN MATCHED AND %s.%s=True THEN DELETE ",
      "WHEN MATCHED AND %s THEN %s ",
      "WHEN NOT MATCHED BY TARGET AND %s.%s!=True ",
      "THEN %s");

  public static String buildMergeStatement(
      String replicaTable,
      String stagingTable,
      List<String> primaryKeyFields,
      List<String> allFields,
      String timestampField,
      String deletedField,
      Integer daysOfRetention) {
    return String.format(MERGE_TEMPLATE,
        replicaTable, REPLICA_TABLE_NAME,
        buildLatestViewOfStagingTable(stagingTable, allFields, primaryKeyFields,
            timestampField, deletedField, daysOfRetention), STAGING_TABLE_NAME,
        buildJoinConditions(primaryKeyFields, REPLICA_TABLE_NAME, STAGING_TABLE_NAME),
        STAGING_TABLE_NAME, deletedField,
        buildTimestampCheck(timestampField), buildUpdateStatement(allFields),
        STAGING_TABLE_NAME, deletedField,
        buildInsertStatement(allFields));
  }

  static String buildTimestampCheck(String timestampField) {
    return String.format("%s.%s <= %s.%s",
        REPLICA_TABLE_NAME, timestampField, STAGING_TABLE_NAME, timestampField);
  }

  public static final String LATEST_FROM_STAGING_TEMPLATE = "SELECT %s FROM (%s) WHERE row_num=1";

  static String buildLatestViewOfStagingTable(
      String stagingTable, List<String> allFields, List<String> primaryKeyFields,
      String timestampField, String deletedField, Integer daysOfRetention) {
    String commaSeparatedFields = String.join(", ", allFields);

    return String.format(LATEST_FROM_STAGING_TEMPLATE,
        commaSeparatedFields, buildPartitionedByPKAndSorted(stagingTable, allFields,
            primaryKeyFields, timestampField, deletedField, daysOfRetention));
  }

  public static final String PARTITION_BY_PK_AND_SORT_TEMPLATE = String.join("",
      "SELECT %s, ROW_NUMBER() OVER (",
      "PARTITION BY %s ",
      "ORDER BY %s DESC, %s ASC) as row_num ",
      "FROM `%s` WHERE %s");

  static String buildPartitionedByPKAndSorted(
      String stagingTable, List<String> allFields, List<String> primaryKeyFields,
      String timestampField, String deletedField, Integer daysOfRetention) {
    String commaSeparatedFields = String.join(", ", allFields);
    String commaSeparatedPKFields = String.join(", ", primaryKeyFields);
    return String.format(PARTITION_BY_PK_AND_SORT_TEMPLATE,
        commaSeparatedFields,
        commaSeparatedPKFields,
        timestampField, deletedField,
        stagingTable, buildRetentionWhereClause(daysOfRetention));
  }

  public static final String RETENTION_WHERE_TEMPLATE = String.join("",
      "_PARTITIONTIME >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -%s DAY)) ",
      "OR _PARTITIONTIME IS NULL");
  static String buildRetentionWhereClause(Integer daysOfRetention) {
    return String.format(RETENTION_WHERE_TEMPLATE, daysOfRetention);
  }

  static String buildJoinConditions(
      List<String> primaryKeyFields, final String leftTableName, final String rightTableName) {
    List<String> equalityConditions = primaryKeyFields.stream()
        .map(col -> String.format("%s.%s = %s.%s", leftTableName, col, rightTableName, col))
        .collect(Collectors.toList());
    return String.join(" AND ", equalityConditions);
  }

  static final String UPDATE_STATEMENT = "UPDATE SET %s";

  static String buildUpdateStatement(List<String> allFields) {
    List<String> assignmentStatements = allFields.stream()
        .map(column -> String.format("%s = %s.%s", column, STAGING_TABLE_NAME, column))
        .collect(Collectors.toList());

    return String.format(UPDATE_STATEMENT,
        String.join(", ", assignmentStatements));
  }

  static final String INSERT_STATEMENT = "INSERT(%s) VALUES (%s)";

  static String buildInsertStatement(List<String> allFields) {
    List<String> changelogPrefixedFields = allFields.stream()
        .map(f -> String.format("%s.%s", STAGING_TABLE_NAME, f))
        .collect(Collectors.toList());
    return String.format(INSERT_STATEMENT,
        String.join(", ", allFields),
        String.join(", ", changelogPrefixedFields));
  }
}
