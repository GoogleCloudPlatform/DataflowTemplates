/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for loading BigQuery metadata for tables in a dataset, including schema,
 * partitioning information, and last modification time.
 *
 * <p>Note: {@link BigQueryMetadataLoader} won't automatically close the {@link
 * BigQueryStorageClient} provided in the constructor, it's the caller's responsibility to do that
 * to clean up resources when the {@link BigQueryStorageClient} is no longer needed.
 */
public class BigQueryMetadataLoader {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetadataLoader.class);

  private final BigQuery bqClient;
  private final BigQueryStorageClient bqsClient;
  private final int maxParallelRequests;

  public BigQueryMetadataLoader(
      BigQuery bqClient, BigQueryStorageClient bqsClient, int maxParallelRequests) {
    this.bqClient = bqClient;
    this.bqsClient = bqsClient;
    this.maxParallelRequests = maxParallelRequests;
  }

  protected boolean shouldSkipUnpartitionedTable(BigQueryTable.Builder table) {
    return false;
  }

  protected boolean shouldSkipPartitionedTable(
      BigQueryTable.Builder table, List<BigQueryTablePartition> partitions) {
    return false;
  }

  protected boolean shouldSkipPartition(
      BigQueryTable.Builder table, BigQueryTablePartition partition) {
    return false;
  }

  /**
   * Loads metadata for all tables in the dataset {@code datasetId} returning only those that match
   * the template filters and have anything to load (e.g. if it's a partitioned table and beforeDate
   * param is set, but all partitioned were modified after beforeDate, it will skip the whole
   * table).
   */
  public List<BigQueryTable> loadDatasetMetadata(DatasetId datasetId)
      throws InterruptedException, ExecutionException {

    String tableSql =
        String.format(
            "select\n"
                + "    table_id,\n"
                + "    timestamp_millis(last_modified_time) as last_modified_time,\n"
                + "    (select column_name from `%s.%s.INFORMATION_SCHEMA.COLUMNS` c\n"
                + "      where c.table_catalog = t.project_id\n"
                + "        and c.table_schema = t.dataset_id\n"
                + "        and c.table_name = t.table_id\n"
                + "        and c.is_partitioning_column = 'YES') as partitioning_column,\n"
                + "  from `%s.%s.__TABLES__` t",
            datasetId.getProject(),
            datasetId.getDataset(),
            datasetId.getProject(),
            datasetId.getDataset());
    TableResult tableRows = bqClient.query(QueryJobConfiguration.newBuilder(tableSql).build());

    List<Callable<BigQueryTable>> tableQueries = new ArrayList<>();

    tableRows
        .iterateAll()
        .forEach(
            row ->
                tableQueries.add(
                    () -> {
                      BigQueryTable.Builder table =
                          BigQueryTable.builder()
                              .setProject(datasetId.getProject())
                              .setDataset(datasetId.getDataset())
                              .setTableName(row.get(0).getStringValue())
                              .setLastModificationTime(row.get(1).getTimestampValue())
                              .setPartitioningColumn(
                                  !row.get(2).isNull() ? row.get(2).getStringValue() : null);

                      if (!loadTableMetadata(table)) {
                        return null;
                      }

                      return table.build();
                    }));

    ExecutorService executor = Executors.newFixedThreadPool(maxParallelRequests);
    List<Future<BigQueryTable>> tableFutures = executor.invokeAll(tableQueries);
    executor.shutdown();

    List<BigQueryTable> tables = new ArrayList<>(tableFutures.size());
    for (Future<BigQueryTable> ft : tableFutures) {
      BigQueryTable t = ft.get();
      if (t != null) {
        tables.add(t);
      }
    }
    return tables;
  }

  /**
   * Populates {@code table} builder with additional metadata like partition names and schema.
   *
   * @return {@code true} if the table matches all filters and should be included in the results,
   *     {@code false} if it should be skipped
   */
  private boolean loadTableMetadata(BigQueryTable.Builder table) throws InterruptedException {
    TableReadOptions.Builder readOptions = TableReadOptions.newBuilder();

    if (table.getPartitioningColumn() == null) {
      if (shouldSkipUnpartitionedTable(table)) {
        return false;
      }
    } else {
      List<BigQueryTablePartition> partitions = loadTablePartitions(table);
      if (shouldSkipPartitionedTable(table, partitions)) {
        return false;
      }

      table.setPartitions(partitions);
      LOG.info(
          "Loaded {} partitions for table {}: {}",
          partitions.size(),
          table.getTableName(),
          partitions);

      // Creating a ReadSession without a WHERE clause for a partitioned table that has
      // "require partition filter" param set to true would fail with the error:
      // "Cannot query over table ... without a filter over column(s) ...
      // that can be used for partition elimination".
      // The following is a hack that adds an "is null and is not null" filter over the
      // partitioning column, which shouldn't select any data but should make the query
      // analyzer happy and should be enough to extract the table schema.
      // TODO(an2x): do this only when "require partition filter" = true
      //             or load schema differently?
      readOptions.setRowRestriction(
          String.format(
              "%s is null and %s is not null",
              table.getPartitioningColumn(), table.getPartitioningColumn()));
    }

    ReadSession session =
        BigQueryUtils.createReadSession(
            bqsClient,
            DatasetId.of(table.getProject(), table.getDataset()),
            table.getTableName(),
            readOptions.build());

    table.setSchema(new Schema.Parser().parse(session.getAvroSchema().getSchema()));
    LOG.info("Loaded schema for table {}: {}", table.getTableName(), table.getSchema());

    return true;
  }

  private List<BigQueryTablePartition> loadTablePartitions(BigQueryTable.Builder table)
      throws InterruptedException {

    String partitionSql =
        String.format(
            "select partition_id, last_modified_time\n"
                + "from `%s.%s.INFORMATION_SCHEMA.PARTITIONS`\n"
                + "where table_name = @table_name",
            table.getProject(), table.getDataset());

    TableResult partitionRows =
        bqClient.query(
            QueryJobConfiguration.newBuilder(partitionSql)
                .addNamedParameter("table_name", QueryParameterValue.string(table.getTableName()))
                .build());

    List<BigQueryTablePartition> partitions = new ArrayList<>();
    partitionRows
        .iterateAll()
        .forEach(
            // TODO(an2x): Check we didn't get duplicate partition names.
            r -> {
              BigQueryTablePartition p =
                  BigQueryTablePartition.builder()
                      .setPartitionName(r.get(0).getStringValue())
                      .setLastModificationTime(r.get(1).getTimestampValue())
                      .build();
              if (!shouldSkipPartition(table, p)) {
                partitions.add(p);
              }
            });
    return partitions;
  }
}
