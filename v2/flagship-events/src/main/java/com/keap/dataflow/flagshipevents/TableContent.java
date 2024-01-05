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
package com.keap.dataflow.flagshipevents;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import java.io.Serializable;

public class TableContent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String serializedTableRow;
  private String serializedTableSchema;
  private String tableName;

  public TableRow getTableRow() {
    return new Gson().fromJson(serializedTableRow, TableRow.class);
  }

  public void setTableRow(TableRow tableRow) {
    this.serializedTableRow = new Gson().toJson(tableRow);
  }

  public TableSchema getTableSchema() {
    return new Gson().fromJson(serializedTableSchema, TableSchema.class);
  }

  public void setTableSchema(TableSchema tableSchema) {
    this.serializedTableSchema = new Gson().toJson(tableSchema);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
