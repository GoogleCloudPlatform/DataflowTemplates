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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

/** Class {@link MergeStatementBuilder}. */
public class MergeStatementBuilder implements Serializable {

  public static final String STAGING_TABLE_NAME = "staging";
  public static final String REPLICA_TABLE_NAME = "replica";

  private final MergeConfiguration configuration;

  public MergeStatementBuilder(MergeConfiguration configuration) {
    this.configuration = configuration;
  }

  public String buildMergeStatement(
      String replicaTable,
      String stagingTable,
      List<String> primaryKeyFields,
      List<String> orderByFields,
      String deletedFieldName,
      List<String> allFields) {
    // Key/Value Map used to replace values in template
    Map<String, String> mergeQueryValues = new HashMap<>();

    mergeQueryValues.put("replicaTable", replicaTable);
    mergeQueryValues.put("replicaAlias", REPLICA_TABLE_NAME);
    mergeQueryValues.put("stagingAlias", STAGING_TABLE_NAME);
    mergeQueryValues.put("deleteColumn", deletedFieldName);

    mergeQueryValues.put(
        "stagingViewSql",
        buildLatestViewOfStagingTable(
            stagingTable,
            allFields,
            primaryKeyFields,
            orderByFields,
            deletedFieldName,
            configuration.partitionRetention()));

    mergeQueryValues.put(
        "joinCondition",
        buildJoinConditions(primaryKeyFields, REPLICA_TABLE_NAME, STAGING_TABLE_NAME));
    mergeQueryValues.put(
        "timestampCompareSql", buildTimestampCheck(getPrimarySortField(orderByFields)));
    mergeQueryValues.put(
        "mergeUpdateSql", buildUpdateStatement(allFields, configuration.quoteCharacter()));
    mergeQueryValues.put(
        "mergeInsertSql", buildInsertStatement(allFields, configuration.quoteCharacter()));

    String mergeStatement =
        StringSubstitutor.replace(configuration.mergeQueryTemplate(), mergeQueryValues, "{", "}");
    return mergeStatement;
  }

  public static String getPrimarySortField(List<String> orderByFields) {
    return orderByFields.get(0);
  }

  static String buildTimestampCheck(String timestampField) {
    return String.format(
        "%s.%s <= %s.%s", REPLICA_TABLE_NAME, timestampField, STAGING_TABLE_NAME, timestampField);
  }

  public static final String LATEST_FROM_STAGING_TEMPLATE = "SELECT %s FROM (%s) WHERE row_num=1";

  private String buildLatestViewOfStagingTable(
      String stagingTable,
      List<String> allFields,
      List<String> primaryKeyFields,
      List<String> orderByFields,
      String deletedFieldName,
      Integer daysOfRetention) {
    String commaSeparatedFields = joinStringFields(",", allFields, "`");

    return String.format(
        LATEST_FROM_STAGING_TEMPLATE,
        commaSeparatedFields,
        buildPartitionedByPKAndSorted(
            stagingTable, allFields, primaryKeyFields, orderByFields, deletedFieldName));
  }

  private static String joinStringFields(String delimiter, List<String> fields, String quoteChar) {
    List<String> quotedFields = new ArrayList<String>();
    for (String field : fields) {
      quotedFields.add(quoteChar + field + quoteChar);
    }

    return String.join(delimiter, quotedFields);
  }

  public static final String PARTITION_BY_PK_AND_SORT_TEMPLATE =
      String.join(
          "",
          "SELECT %s, ROW_NUMBER() OVER (",
          "PARTITION BY %s ",
          "ORDER BY %s%s) as row_num ",
          "FROM `%s` %s");

  private String buildPartitionedByPKAndSorted(
      String stagingTable,
      List<String> allFields,
      List<String> primaryKeyFields,
      List<String> orderByFields,
      String deletedFieldName) {
    String commaSeparatedFields = joinStringFields(",", allFields, configuration.quoteCharacter());
    String commaSeparatedPKFields = String.join(", ", primaryKeyFields);
    return String.format(
        PARTITION_BY_PK_AND_SORT_TEMPLATE,
        commaSeparatedFields,
        commaSeparatedPKFields,
        buildOrderByFieldsSql(orderByFields),
        buildDeletedFieldSql(deletedFieldName),
        stagingTable,
        buildRetentionWhereClause());
  }

  private String buildOrderByFieldsSql(List<String> orderByFields) {
    String orderByFieldSql = "";

    for (String field : orderByFields) {
      if (orderByFieldSql == "") {
        orderByFieldSql = String.format("%s DESC", field);
      } else {
        orderByFieldSql = orderByFieldSql + String.format(", %s DESC", field);
      }
    }

    return orderByFieldSql;
  }

  private String buildDeletedFieldSql(String deletedFieldName) {
    return String.format(", %s ASC", deletedFieldName);
  }

  public static final String RETENTION_WHERE_TEMPLATE =
      String.join(
          "",
          "WHERE _PARTITIONTIME >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -%s DAY)) ",
          "OR _PARTITIONTIME IS NULL");

  String buildRetentionWhereClause() {
    if (configuration.supportPartitionedTables()) {
      return String.format(RETENTION_WHERE_TEMPLATE, configuration.partitionRetention());
    } else {
      return "";
    }
  }

  static String buildJoinConditions(
      List<String> primaryKeyFields, final String leftTableName, final String rightTableName) {
    List<String> equalityConditions =
        primaryKeyFields.stream()
            .map(col -> String.format("%s.%s = %s.%s", leftTableName, col, rightTableName, col))
            .collect(Collectors.toList());
    return String.join(" AND ", equalityConditions);
  }

  static final String UPDATE_STATEMENT = "UPDATE SET %s";

  static String buildUpdateStatement(List<String> allFields, String quoteChar) {
    List<String> assignmentStatements =
        allFields.stream()
            .map(
                column ->
                    String.format(
                        "%s%s%s = %s.%s", quoteChar, column, quoteChar, STAGING_TABLE_NAME, column))
            .collect(Collectors.toList());

    return String.format(UPDATE_STATEMENT, String.join(", ", assignmentStatements));
  }

  static final String INSERT_STATEMENT = "INSERT(%s) VALUES (%s)";

  static String buildInsertStatement(List<String> allFields, String quoteChar) {
    List<String> changelogPrefixedFields =
        allFields.stream()
            .map(f -> String.format("%s.%s", STAGING_TABLE_NAME, f))
            .collect(Collectors.toList());
    return String.format(
        INSERT_STATEMENT,
        String.join(", ", joinStringFields(",", allFields, quoteChar)),
        String.join(", ", changelogPrefixedFields));
  }
}
