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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.ResourceManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Iceberg resources for integration tests.
 *
 * <p>The class supports one catalog, and multiple tables per catalog object.
 *
 * <p>The class is thread-safe.
 */
public class IcebergResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergResourceManager.class);
  private static final String DEFAULT_CATALOG_NAME = "default";

  private final String testId;
  private Catalog cachedCatalog;
  private final String catalogName;
  private final Map<String, String> catalogProps;
  private final Map<String, String> configProps;

  /**
   * Creates a new IcebergResourceManager.
   *
   * @param builder The builder for this resource manager.
   */
  private IcebergResourceManager(Builder builder) {
    this.testId = builder.testId;
    this.catalogName = builder.catalogName != null ? builder.catalogName : DEFAULT_CATALOG_NAME;
    this.catalogProps =
        builder.catalogProperties != null ? builder.catalogProperties : new HashMap<>();
    this.configProps = builder.configProps != null ? builder.configProps : new HashMap<>();
  }

  /**
   * Creates a builder for {@link IcebergResourceManager}.
   *
   * @param testId The ID of the test.
   * @return A new builder.
   */
  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /**
   * Returns the Iceberg catalog. If the catalog is not initialized, it will be built using the
   * provided properties.
   *
   * @return The Iceberg catalog.
   */
  public Catalog catalog() {
    if (cachedCatalog == null) {
      String catalogName = this.catalogName;
      Configuration config = new Configuration();
      for (Map.Entry<String, String> prop : configProps.entrySet()) {
        config.set(prop.getKey(), prop.getValue());
      }
      cachedCatalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, config);
    }
    return cachedCatalog;
  }

  /**
   * Creates a namespace in the Iceberg catalog.
   *
   * @param namespace The name of the namespace to create.
   * @return True if the namespace was created, false if it already exists.
   * @throws IllegalStateException if the catalog does not support namespaces.
   */
  public boolean createNamespace(String namespace) {
    checkSupportsNamespaces();
    String[] components = Iterables.toArray(Splitter.on('.').split(namespace), String.class);

    try {
      ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(components));
      return true;
    } catch (AlreadyExistsException e) {
      return false;
    }
  }

  /**
   * Checks if a namespace exists in the Iceberg catalog.
   *
   * @param namespace The name of the namespace to check.
   * @return True if the namespace exists, false otherwise.
   * @throws IllegalStateException if the catalog does not support namespaces.
   */
  public boolean namespaceExists(String namespace) {
    checkSupportsNamespaces();
    return ((SupportsNamespaces) catalog()).namespaceExists(Namespace.of(namespace));
  }

  /**
   * Lists all namespaces in the Iceberg catalog.
   *
   * @return A set of namespace names.
   * @throws IllegalStateException if the catalog does not support namespaces.
   */
  public Set<String> listNamespaces() {
    checkSupportsNamespaces();

    return ((SupportsNamespaces) catalog())
        .listNamespaces().stream().map(Namespace::toString).collect(Collectors.toSet());
  }

  /**
   * Drops a namespace from the Iceberg catalog.
   *
   * @param namespace The name of the namespace to drop.
   * @param cascade If true, all tables within the namespace will be dropped first.
   * @return True if the namespace was dropped, false if it did not exist.
   * @throws IllegalStateException if the catalog does not support namespaces.
   */
  public boolean dropNamespace(String namespace, boolean cascade) {
    checkSupportsNamespaces();

    String[] components = Iterables.toArray(Splitter.on('.').split(namespace), String.class);
    Namespace ns = Namespace.of(components);

    if (!((SupportsNamespaces) catalog()).namespaceExists(ns)) {
      return false;
    }

    // Cascade will delete all contained tables first
    if (cascade) {
      catalog().listTables(ns).forEach(catalog()::dropTable);
    }

    // Drop the namespace
    return ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(components));
  }

  /**
   * Creates an Iceberg table.
   *
   * @param tableIdentifier The identifier of the table to create (e.g., "database.table_name").
   * @param tableSchema The schema of the table.
   * @throws IcebergResourceManagerException if the table already exists or there is another error.
   */
  public Table createTable(String tableIdentifier, Schema tableSchema) {
    TableIdentifier icebergIdentifier = TableIdentifier.parse(tableIdentifier);
    try {
      return catalog().createTable(icebergIdentifier, tableSchema);
    } catch (AlreadyExistsException e) {
      throw new IcebergResourceManagerException(
          "Table already Exists '" + tableIdentifier + "'.", e);
    }
  }

  /**
   * Loads an Iceberg table.
   *
   * @param tableIdentifier The identifier of the table to load (e.g., "database.table_name").
   * @return The loaded table.
   * @throws IcebergResourceManagerException if the table does not exist or there is another error.
   */
  public Table loadTable(String tableIdentifier) {
    TableIdentifier icebergIdentifier = TableIdentifier.parse(tableIdentifier);
    try {
      return catalog().loadTable(icebergIdentifier);
    } catch (NoSuchTableException e) {
      throw new IcebergResourceManagerException(
          "No Such Table found with name" + tableIdentifier + "'.", e);
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

        DataWriter<Record> icebergDataWriter =
            Parquet.writeData(outputFile)
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(table.spec())
                .schema(table.schema())
                .overwrite()
                .build();
        icebergDataWriter.write(record);
        icebergDataWriter.close();

        DataFile dataFile = icebergDataWriter.toDataFile();

        AppendFiles update = table.newAppend();
        update.appendFile(dataFile);
        update.commit();
      }
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
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      return StreamSupport.stream(records.spliterator(), false).collect(Collectors.toList());
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
    Set<String> namespaces = listNamespaces();
    for (String namespace : namespaces) {
      dropNamespace(namespace, true);
    }
    LOG.info("Cleaned up all resources for test ID: {}.", testId);
  }

  /**
   * Checks if the current catalog supports namespace operations.
   *
   * @throws IllegalStateException if the catalog does not support namespaces.
   */
  private void checkSupportsNamespaces() {
    Preconditions.checkState(
        catalog() instanceof SupportsNamespaces,
        "Catalog '%s' does not support handling namespaces.",
        catalog().name());
  }

  /** Builder for {@link IcebergResourceManager}. */
  public static final class Builder {

    private final String testId;
    private String catalogName;
    private Map<String, String> catalogProperties;
    private Map<String, String> configProps;

    /**
     * Creates a new Builder for {@link IcebergResourceManager}.
     *
     * @param testId The ID of the test.
     */
    private Builder(String testId) {
      this.testId = testId;
    }

    /**
     * Sets the catalog name for the resource manager.
     *
     * @param catalogName The name of the catalog.
     * @return The builder instance.
     */
    public Builder setCatalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    /**
     * Sets the catalog properties for the resource manager.
     *
     * @param catalogProperties A map of catalog properties.
     * @return The builder instance.
     */
    public Builder setCatalogProperties(Map<String, String> catalogProperties) {
      this.catalogProperties = catalogProperties;
      return this;
    }

    /**
     * Sets the configuration properties for Hadoop.
     *
     * @param configProps A map of configuration properties.
     * @return The builder instance.
     */
    public Builder setConfigProperties(Map<String, String> configProps) {
      this.configProps = configProps;
      return this;
    }

    /**
     * Builds a new {@link IcebergResourceManager} instance.
     *
     * @return A new {@link IcebergResourceManager} instance.
     */
    public IcebergResourceManager build() {
      return new IcebergResourceManager(this);
    }
  }
}
