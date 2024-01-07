/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.cdc.mappers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * BigQueryTableRowCleaner cleans TableRow data to ensure the datatypes match the table in BigQuery.
 *
 * <p>BigQueryTableRowCleaner focuses on ensuring the default String mappings match required loading
 * standards for BigQuery.
 */
public class BigQueryTableRowCleaner {

  /** Returns {@code BigQueryTableRowCleaner} used to clean TableRow data. */
  public static BigQueryTableRowCleaner getBigQueryTableRowCleaner() {
    return new BigQueryTableRowCleaner();
  }

  /**
   * Cleans the TableRow data for a given rowKey if the value does not match requirements of the
   * BigQuery Table column type.
   *
   * @param row a TableRow object to clean.
   * @param tableFields a FieldList of Bigquery columns.
   * @param rowKey a String with the name of the field to clean.
   */
  public static void cleanTableRowField(TableRow row, FieldList tableFields, String rowKey) {
    Field tableField = tableFields.get(rowKey);
    LegacySQLTypeName fieldType = tableField.getType();

    if (fieldType == LegacySQLTypeName.STRING) {
      cleanTableRowFieldStrings(row, tableFields, rowKey);
    } else if (fieldType == LegacySQLTypeName.DATE) {
      cleanTableRowFieldDates(row, tableFields, rowKey);
    } else if (fieldType == LegacySQLTypeName.DATETIME) {
      cleanTableRowFieldDateTime(row, tableFields, rowKey);
    }
  }

  /**
   * Cleans the TableRow data for a given rowKey based on the requirements of a BigQuery String
   * column type.
   *
   * @param row a TableRow object to clean.
   * @param tableFields a FieldList of Bigquery columns.
   * @param rowKey a String with the name of the field to clean.
   */
  public static void cleanTableRowFieldStrings(TableRow row, FieldList tableFields, String rowKey) {
    Object rowObject = row.get(rowKey);
    Gson gson = new Gson();
    if (rowObject instanceof Boolean) {
      Boolean rowValue = (Boolean) rowObject;
      row.put(rowKey, rowValue.toString());
    } else if (rowObject instanceof LinkedHashMap) {
      String jsonString = gson.toJson(rowObject, Map.class);
      row.put(rowKey, jsonString);
    } else if (rowObject instanceof ArrayList) {
      String jsonString = gson.toJson(rowObject);
      row.put(rowKey, jsonString);
    }
  }

  /**
   * Cleans the TableRow data for a given rowKey based on the requirements of a BigQuery DATE column
   * type.
   *
   * @param row a TableRow object to clean.
   * @param tableFields a FieldList of Bigquery columns.
   * @param rowKey a String with the name of the field to clean.
   */
  public static void cleanTableRowFieldDates(TableRow row, FieldList tableFields, String rowKey) {
    Object rowObject = row.get(rowKey);
    if (rowObject instanceof String) {
      String dateString = (String) rowObject;
      // Split only the date portion if value is a timestamp
      if (dateString.contains("T00:00:00Z")) {
        row.put(rowKey, dateString.replace("T00:00:00Z", ""));
      } else if (dateString.contains("T00:00:00.000")) {
        row.put(rowKey, dateString.replace("T00:00:00.000", ""));
      }
    }
  }

  /**
   * Cleans the TableRow data for a given rowKey based on the requirements of a BigQuery DATETIME
   * column type.
   *
   * @param row a TableRow object to clean.
   * @param tableFields a FieldList of Bigquery columns.
   * @param rowKey a String with the name of the field to clean.
   */
  public static void cleanTableRowFieldDateTime(
      TableRow row, FieldList tableFields, String rowKey) {
    Object rowObject = row.get(rowKey);
    if (rowObject instanceof String) {
      String dateTimeString = (String) rowObject;
      // Datetime types do not allow Z which resprents UTC timezone info
      if (dateTimeString.endsWith("Z")) {
        row.put(rowKey, dateTimeString.substring(0, dateTimeString.length() - 1));
      }
    }
  }
}
