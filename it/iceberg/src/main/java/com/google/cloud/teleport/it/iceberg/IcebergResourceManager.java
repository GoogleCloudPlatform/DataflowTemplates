/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.it.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.ResourceManager;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Iceberg resources.
 *
 * <p>The class supports one catalog, and multiple tables per catalog object.
 *
 * <p>The class is thread-safe.
 */
public class IcebergResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergResourceManager.class);
  private static final String DEFAULT_CATALOG_NAME = "default";

  private final String testId;
  private final Catalog catalog;
  private final String catalogName;

  private IcebergResourceManager(Builder builder) {
    this.testId = builder.testId;
    this.catalogName = builder.catalogName != null ? builder.catalogName : DEFAULT_CATALOG_NAME;
    this.catalog = createIcebergCatalog(this.catalogName, builder.catalogProperties, builder.conf);

    LOG.info("Initialized Iceberg resource manager with catalog '{}'.", catalogName);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /**
   * Creates an Iceberg table in the catalog.
   *
   * @param tableName The name of the table to create.
   * @param schema The schema of the table.
   * @return The created Table object.
   * @throws IcebergResourceManagerException if there is an error creating the table.
   */
  public Table createTable(String tableName, Schema schema) {
    TableIdentifier tableIdentifier = TableIdentifier.of(tableName);
    LOG.info("Creating Iceberg table '{}' in catalog '{}'.", tableName, catalogName);
    try {
      Table table = catalog.createTable(tableIdentifier, schema);
      LOG.info("Successfully created Iceberg table '{}'.", tableName);
      return table;
    } catch (Exception e) {
      throw new IcebergResourceManagerException(
          "Failed to create Iceberg table '" + tableName + "'.", e);
    }
  }

  /**
   * Loads an existing Iceberg table from the catalog.
   *
   * @param tableName The name of the table to load.
   * @return The loaded Table object.
   * @throws IcebergResourceManagerException if the table does not exist or there is an error
   *     loading it.
   */
  public Table loadTable(String tableName) {
    TableIdentifier tableIdentifier = TableIdentifier.of(tableName);
    LOG.info("Loading Iceberg table '{}' from catalog '{}'.", tableName, catalogName);
    try {
      Table table = catalog.loadTable(tableIdentifier);
      LOG.info("Successfully loaded Iceberg table '{}'.", tableName);
      return table;
    } catch (Exception e) {
      throw new IcebergResourceManagerException(
          "Failed to load Iceberg table '" + tableName + "'.", e);
    }
  }

  /**
   * Writes a list of records to an Iceberg table.
   *
   * @param tableName The name of the table to write to.
   * @param records The list of records to write.
   * @throws IcebergResourceManagerException if there is an error writing to the table.
   */
  public void write(String tableName, List<Map<String, Object>> records) {
    Table table = loadTable(tableName);
    LOG.info("Writing {} records to Iceberg table '{}'.", records.size(), tableName);

    FileIO io = table.io();
    String warehouseLocation = table.location();

    try {
      for (Map<String, Object> recordMap : records) {
        GenericRecord record = GenericRecord.create(table.schema());
        for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
          record.setField(entry.getKey(), entry.getValue());
        }

        String filePath = warehouseLocation + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = io.newOutputFile(filePath);

        FileAppender<Record> appender =
            Parquet.write(outputFile)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(table.schema())
                .overwrite()
                .build();
        appender.add(record);
        appender.close();

        InputFile inputFile = io.newInputFile(filePath);
        DataFile dataFile =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(inputFile)
                .withMetrics(appender.metrics())
                .build();

        table.newFastAppend().appendFile(dataFile).commit();
      }
      LOG.info("Successfully wrote {} records to Iceberg table '{}'.", records.size(), tableName);
    } catch (IOException e) {
      throw new IcebergResourceManagerException(
          "Failed to write records to Iceberg table '" + tableName + "'.", e);
    }
  }

  /**
   * Reads all records from an Iceberg table.
   *
   * @param tableName The name of the table to read from.
   * @return A list of records read from the table.
   * @throws IcebergResourceManagerException if there is an error reading from the table.
   */
  public List<Record> read(String tableName) {
    Table table = loadTable(tableName);
    LOG.info("Reading all records from Iceberg table '{}'.", tableName);
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      List<Record> result =
          StreamSupport.stream(records.spliterator(), false).collect(Collectors.toList());
      LOG.info("Successfully read {} records from Iceberg table '{}'.", result.size(), tableName);
      return result;
    } catch (IOException e) {
      throw new IcebergResourceManagerException(
          "Failed to read records from Iceberg table '" + tableName + "'.", e);
    }
  }

  /**
   * Deletes all created resources (tables) and cleans up the Iceberg client, making the manager
   * object unusable.
   *
   * @throws IcebergResourceManagerException if there is an error deleting the resources.
   */
  @Override
  public synchronized void cleanupAll() throws IcebergResourceManagerException {
    LOG.info("Attempting to cleanup Iceberg resource manager for test ID: {}.", testId);
    try {
      // List and drop all tables in the catalog
      List<TableIdentifier> tableIdentifiers =
          catalog.listTables(Namespace.empty()); // List all tables in the default namespace
      for (TableIdentifier tableIdentifier : tableIdentifiers) {
        catalog.dropTable(tableIdentifier);
        LOG.info("Dropped Iceberg table: {}", tableIdentifier);
      }
    } catch (Exception e) {
      throw new IcebergResourceManagerException("Failed to cleanup Iceberg resources.", e);
    }
    LOG.info("Iceberg resource manager successfully cleaned up.");
  }

  private Catalog createIcebergCatalog(
      String catalogName, Map<String, String> catalogProperties, Object conf) {
    if (catalogProperties == null || catalogProperties.isEmpty()) {
      throw new IcebergResourceManagerException(
          "Catalog properties must be provided to create an Iceberg Catalog.");
    }

    try {
      return CatalogUtil.buildIcebergCatalog(catalogName, catalogProperties, conf);
    } catch (Exception e) {
      throw new IcebergResourceManagerException(
          "Failed to create Iceberg catalog '" + catalogName + "'.", e);
    }
  }

  /** Builder for {@link IcebergResourceManager}. */
  public static final class Builder {

    private final String testId;
    private String catalogName;
    private Map<String, String> catalogProperties;
    private Object conf;

    private Builder(String testId) {
      this.testId = testId;
    }

    public Builder setCatalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public Builder withCatalogProperties(Map<String, String> catalogProperties) {
      this.catalogProperties = catalogProperties;
      return this;
    }

    public Builder withConf(Object conf) {
      this.conf = conf;
      return this;
    }

    public IcebergResourceManager build() {
      return new IcebergResourceManager(this);
    }
  }
}
