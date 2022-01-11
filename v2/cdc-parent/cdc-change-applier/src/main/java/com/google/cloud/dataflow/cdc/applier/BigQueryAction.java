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

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Represents an action against BigQuery to be performed by {@link BigQueryStatementIssuingFn}.
 *
 * <p>Currently there are two types of actions: - Table creation. - Statement execution.
 *
 * <p>Table creation actions receive a full table name, and a table schema; and create a new table
 * in BigQuery.
 *
 * <p>Statement execution actions provide only a SQL statement, and the statement is sent to
 * BigQuery directly.
 */
public class BigQueryAction implements Serializable {
  public static final String CREATE_TABLE = "CREATE_TABLE";
  public static final String STATEMENT = "STATEMENT";

  public final String action;
  public final String statement;
  public final Schema tableSchema;
  public final String projectId;
  public final String dataset;
  public final String tableName;

  private BigQueryAction(
      String action,
      String statement,
      String projectId,
      String dataset,
      String tableName,
      Schema tableSchema) {
    this.action = action;
    this.statement = statement;
    this.tableName = tableName;
    this.projectId = projectId;
    this.dataset = dataset;
    this.tableSchema = tableSchema;
  }

  static BigQueryAction createTable(
      String projectId, String dataset, String tableName, Schema tableSchema) {
    return new BigQueryAction(
        BigQueryAction.CREATE_TABLE, null, projectId, dataset, tableName, tableSchema);
  }

  static BigQueryAction query(String statement) {
    return new BigQueryAction(BigQueryAction.STATEMENT, statement, null, null, null, null);
  }
}
