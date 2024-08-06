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
package com.google.cloud.teleport.v2.utils;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Provides functionality to interact with Spanner. */
@AutoValue
public abstract class SpannerQueryHelper {

  public static SpannerQueryHelper create(SpannerConfig spannerConfig) {
    return new AutoValue_SpannerQueryHelper(spannerConfig);
  }

  abstract SpannerConfig spannerConfig();

  /**
   * Creates a new Spanner database client.
   *
   * @return Spanner database client.
   */
  public DatabaseClient createDatabaseClient() {
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig());
    return spannerAccessor.getDatabaseClient();
  }

  /**
   * Generates the Key for a given record and (primary) key column names.
   *
   * @param record
   * @param primaryKeyColumnNames
   * @return Primary Key for the record.
   */
  public Key createKeyForRecord(Struct record, Iterable<String> primaryKeyColumnNames) {
    Key.Builder keyBuilder = Key.newBuilder();
    addRecordFieldsToKeyBuilder(record, primaryKeyColumnNames, keyBuilder);
    return keyBuilder.build();
  }

  /**
   * Adds struct values to the Key builder for the requested column names.
   *
   * <p>Used to generate Keys for records.
   *
   * @param record
   * @param columnNames to add to the Key.
   * @param keyBuilder
   * @return
   */
  public Key.Builder addRecordFieldsToKeyBuilder(
      Struct record, Iterable<String> columnNames, Key.Builder keyBuilder) {
    HashSet<String> columnNamesSet = new HashSet<>();
    columnNames.forEach(columnNamesSet::add);

    record.getType().getStructFields().stream()
        .filter(field -> columnNamesSet.contains(field.getName()))
        .forEach(
            field -> {
              Value fieldValue = record.getValue(field.getName());
              Type fieldType = fieldValue.getType();

              switch (fieldType.getCode()) {
                case BOOL:
                  keyBuilder.append(StructValueHelper.getBoolOrNull(fieldValue));
                  break;
                case BYTES:
                  keyBuilder.append(StructValueHelper.getBytesOrNull(fieldValue));
                  break;
                case DATE:
                  keyBuilder.append(StructValueHelper.getDateOrNull(fieldValue));
                  break;
                case FLOAT32:
                  keyBuilder.append(StructValueHelper.getFloat32OrNull(fieldValue));
                  break;
                case FLOAT64:
                  keyBuilder.append(StructValueHelper.getFloat64OrNull(fieldValue));
                  break;
                case INT64:
                  keyBuilder.append(StructValueHelper.getInt64OrNull(fieldValue));
                  break;
                case JSON:
                  keyBuilder.append(StructValueHelper.getJsonOrNull(fieldValue));
                  break;
                case NUMERIC:
                case PG_NUMERIC:
                  keyBuilder.append(StructValueHelper.getNumericOrNull(fieldValue));
                  break;
                case PG_JSONB:
                  keyBuilder.append(StructValueHelper.getPgJsonbOrNull(fieldValue));
                  break;
                case STRING:
                  keyBuilder.append(StructValueHelper.getStringOrNull(fieldValue));
                  break;
                case TIMESTAMP:
                  keyBuilder.append(StructValueHelper.getTimestampOrNull(fieldValue));
                  break;
                default:
                  throw new UnsupportedOperationException(
                      String.format("Unsupported Spanner field type %s.", fieldType.getCode()));
              }
            });
    return keyBuilder;
  }

  /**
   * Queries Spanner table.
   *
   * <p>Uses the KeySet as the WHERE clause and the columns in the SELECT.
   *
   * <p>Creates a new database client and transaction.
   *
   * @param tableName
   * @param queryKeySet
   * @param columns
   * @return Matching list of rows (ResultSet) with the requested columns.
   */
  public ResultSet queryRecords(String tableName, KeySet queryKeySet, Iterable<String> columns) {
    ReadOnlyTransaction transaction = createDatabaseClient().readOnlyTransaction();
    return queryRecordsWithTransaction(transaction, tableName, queryKeySet, columns);
  }

  /**
   * Queries Spanner table.
   *
   * <p>Uses the KeySet as the WHERE clause and the columns in the SELECT.
   *
   * <p>Uses the provided transaction for the query.
   *
   * @param transaction
   * @param tableName
   * @param queryKeySet
   * @param columns
   * @return Matching list of rows (ResultSet) with the requested columns.
   */
  public ResultSet queryRecordsWithTransaction(
      ReadContext transaction, String tableName, KeySet queryKeySet, Iterable<String> columns) {
    Iterable<String> queryColumns = columns != null ? columns : getTableColumnNames(tableName);
    return transaction.read(tableName, queryKeySet, queryColumns);
  }

  /**
   * Gets the column names for a given Spanner table name.
   *
   * <p>Always creates a new transaction. It is not possible to use the same transaction to read
   * data and schema.
   *
   * @param tableName
   * @return List of columns for the given table name.
   */
  public Iterable<String> getTableColumnNames(String tableName) {
    DatabaseClient spannerClient = createDatabaseClient();
    String schemaQuery =
        String.format(
            "SELECT COLUMN_NAME FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE TABLE_NAME = \"%s\"",
            tableName);
    ResultSet results = spannerClient.readOnlyTransaction().executeQuery(Statement.of(schemaQuery));

    ArrayList<String> columnNames = new ArrayList<>();
    while (results.next()) {
      Struct rowStruct = results.getCurrentRowAsStruct();
      columnNames.add(rowStruct.getString("COLUMN_NAME"));
    }
    return columnNames;
  }
}
