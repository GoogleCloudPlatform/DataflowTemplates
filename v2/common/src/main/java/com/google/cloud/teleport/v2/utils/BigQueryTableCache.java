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

package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQueryTableCache} manages safely getting and setting BigQuery Table objects from a
 * local cache for each worker thread.
 *
 * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
 * behavior to ensure reliabillity, and easy cache reloads. Open Question: Does the class require
 * thread-safe behaviors? Currently it does not since there is no iteration and get/set are not
 * continuous.
 */
public class BigQueryTableCache
    extends MappedObjectCache<TableId, Table> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableCache.class);
  private BigQuery bigquery;
  private boolean createTableIfDne = false;
  private boolean dayPartitioning = false;

  /**
   * Create an instance of a {@link BigQueryTableCache} to track table schemas.
   *
   * @param bigquery A BigQuery instance used to extract Table objects.
   */
  public BigQueryTableCache(BigQuery bigquery) {
    this.bigquery = bigquery;
  }

  public BigQueryTableCache withCreateTableIfDne(boolean dayPartitioning) {
    this.createTableIfDne = true;
    this.dayPartitioning = dayPartitioning;
    return this;
  }

  @Override
  public Table getObjectValue(TableId key) {
    LOG.info("BigQueryTableCache: Get mapped object cache {}", key.toString());
    Table table = this.bigquery.getTable(key);
    if (table == null && this.createTableIfDne) {
      table = createBigQueryTable(key);
    }
    return table;
  }

  /**
   * Returns {@code Table} after creating the table with no columns in BigQuery.
   *
   * @param tableId a TableId referencing the BigQuery table being requested.
   */
  private Table createBigQueryTable(TableId tableId) {
    // Create Blank BigQuery Table
    List<Field> fieldList = new ArrayList<Field>();
    Schema schema = Schema.of(fieldList);

    StandardTableDefinition.Builder tableDefinitionBuilder =
        StandardTableDefinition.newBuilder().setSchema(schema);
    if (this.dayPartitioning) {
      LOG.info("Creating BQ Table {} using time partitioning", tableId);
      tableDefinitionBuilder.setTimePartitioning(
          TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build());
    }
    LOG.info("Creating BQ Table {} with  schema {}", tableId, schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinitionBuilder.build()).build();
    Table table = bigquery.create(tableInfo);

    return table;
  }
}