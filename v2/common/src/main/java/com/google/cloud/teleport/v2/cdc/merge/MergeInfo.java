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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.utils.BigQueryTableCache;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/** Class {@link MergeInfo}. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MergeInfo implements Serializable {

  private static BigQueryTableCache tableCache;

  public abstract String getProjectId();

  public abstract List<String> getAllPkFields();

  public abstract List<String> getOrderByFields();

  public abstract String getDeleteField();

  public abstract TableId getStagingTable();

  public abstract TableId getReplicaTable();

  public abstract List<String> getCustomColumns();

  public abstract String getJobId();

  public static MergeInfo create(
      String projectId,
      List<String> allPkFields,
      List<String> orderByFields,
      String deleteField,
      TableId stagingTable,
      TableId replicaTable,
      String jobId) {
    return create(
        projectId,
        allPkFields,
        orderByFields,
        deleteField,
        stagingTable,
        replicaTable,
        new ArrayList<>(),
        jobId);
  }

  @SchemaCreate
  public static MergeInfo create(
      String projectId,
      List<String> allPkFields,
      List<String> orderByFields,
      String deleteField,
      TableId stagingTable,
      TableId replicaTable,
      List<String> customColumns,
      String jobId) {
    return new AutoValue_MergeInfo(
        projectId,
        allPkFields,
        orderByFields,
        deleteField,
        stagingTable,
        replicaTable,
        customColumns,
        jobId);
  }

  /** Returns the formatted String reference to the BigQuery replica table. */
  public String getReplicaTableReference() {
    return getTableReference(this.getReplicaTable());
  }

  /** Returns the formatted String reference to the BigQuery staging table. */
  public String getStagingTableReference() {
    return getTableReference(this.getStagingTable());
  }

  /**
   * Returns a Merge SQL string using the merge info and supplied configuration.
   *
   * @param mergeConfiguration contains all the Merge query settings required to build a Merge SQL
   */
  public String buildMergeStatement(MergeConfiguration mergeConfiguration) {
    MergeStatementBuilder mergeBuilder = new MergeStatementBuilder(mergeConfiguration);
    return mergeBuilder.buildMergeStatement(
        getReplicaTableReference(),
        getStagingTableReference(),
        this.getAllPkFields(),
        this.getOrderByFields(),
        this.getDeleteField(),
        this.getColumns());
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || !(object instanceof MergeInfo)) {
      return false;
    }
    MergeInfo other = (MergeInfo) object;
    if (this.getProjectId().equals(other.getProjectId())
        && this.getStagingTable().equals(other.getStagingTable())
        && this.getReplicaTable().equals(other.getReplicaTable())) {
      return true;
    }

    return false;
  }

  private BigQueryTableCache getTableCache() {
    if (this.tableCache == null) {
      setUpTableCache();
    }

    return this.tableCache;
  }

  private synchronized void setUpTableCache() {
    if (tableCache == null) {
      BigQuery bigquery =
          BigQueryOptions.newBuilder().setProjectId(getProjectId()).build().getService();
      tableCache = new BigQueryTableCache(bigquery);
    }
  }

  @VisibleForTesting
  static void setTableCache(BigQueryTableCache newTableCache) {
    tableCache = newTableCache;
  }

  @VisibleForTesting
  static void resetTableCache() {
    tableCache = null;
  }

  private List<String> getColumns() {
    if (getCustomColumns().isEmpty()) {
      return getMergeFields(getReplicaTable());
    }

    return getCustomColumns();
  }

  @VisibleForTesting
  List<String> getMergeFields(TableId tableId) {
    List<String> mergeFields = new ArrayList<String>();
    Table table = getTableCache().get(tableId);
    if (table == null) {
      throw new IllegalArgumentException(
          "Could not get the table '" + getTableReference(tableId) + "' from BigQuery.");
    }
    if (table.getDefinition() == null || table.getDefinition().getSchema() == null) {
      throw new IllegalArgumentException(
          "Could not get the schema for BigQuery table '" + getTableReference(tableId) + "'.");
    }

    FieldList tableFields = table.getDefinition().getSchema().getFields();

    for (Field field : tableFields) {
      mergeFields.add(field.getName());
    }

    return mergeFields;
  }

  public static String getTableReference(TableId tableId) {
    return String.format(
        "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }
}
