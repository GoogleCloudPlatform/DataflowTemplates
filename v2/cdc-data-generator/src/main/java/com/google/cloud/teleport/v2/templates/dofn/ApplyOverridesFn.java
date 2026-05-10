/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to apply schema overrides from a HOCON/JSON config file. */
public class ApplyOverridesFn extends DoFn<DataGeneratorSchema, DataGeneratorSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(ApplyOverridesFn.class);

  private final SchemaConfig schemaConfig;
  private final Integer defaultInsertQps;
  private final Integer defaultUpdateQps;
  private final Integer defaultDeleteQps;

  public ApplyOverridesFn(
      @Nullable SchemaConfig schemaConfig,
      Integer defaultInsertQps,
      Integer defaultUpdateQps,
      Integer defaultDeleteQps) {
    this.schemaConfig = schemaConfig;
    this.defaultInsertQps = defaultInsertQps;
    this.defaultUpdateQps = defaultUpdateQps;
    this.defaultDeleteQps = defaultDeleteQps;
  }

  @ProcessElement
  public void processElement(
      @Element DataGeneratorSchema schema, OutputReceiver<DataGeneratorSchema> receiver) {
    Map<String, SchemaConfig.TableConfig> configTables = SchemaConfig.getTables(schemaConfig);

    // Warn about unknown tables specified in the config override
    configTables.keySet().stream()
        .filter(tableName -> !schema.tables().containsKey(tableName))
        .forEach(tableName -> LOG.warn("Override specified for unknown table: {}", tableName));

    ImmutableMap.Builder<String, DataGeneratorTable> updatedTables = ImmutableMap.builder();
    for (DataGeneratorTable table : schema.tables().values()) {
      updatedTables.put(table.name(), applyTableOverrides(table, configTables.get(table.name())));
    }

    receiver.output(DataGeneratorSchema.builder().tables(updatedTables.build()).build());
  }

  private DataGeneratorTable applyTableOverrides(
      DataGeneratorTable table, @Nullable SchemaConfig.TableConfig tableConfig) {
    DataGeneratorTable.Builder tableBuilder = table.toBuilder();

    // Resolve QPS values hierarchically: Table-level Override -> Global Default
    tableBuilder.insertQps(
        Optional.ofNullable(tableConfig)
            .map(SchemaConfig.TableConfig::getInsertQps)
            .orElse(defaultInsertQps));

    tableBuilder.updateQps(
        Optional.ofNullable(tableConfig)
            .map(SchemaConfig.TableConfig::getUpdateQps)
            .orElse(defaultUpdateQps));

    tableBuilder.deleteQps(
        Optional.ofNullable(tableConfig)
            .map(SchemaConfig.TableConfig::getDeleteQps)
            .orElse(defaultDeleteQps));

    if (tableConfig != null) {
      if (tableConfig.getColumns() != null) {
        tableBuilder.columns(applyColumnOverrides(table, tableConfig.getColumns()));
      }
      if (tableConfig.getForeignKeys() != null) {
        tableBuilder.foreignKeys(mergeForeignKeys(table, tableConfig.getForeignKeys()));
      }
    }

    return tableBuilder.build();
  }

  private ImmutableList<DataGeneratorColumn> applyColumnOverrides(
      DataGeneratorTable existingTable, Map<String, SchemaConfig.ColumnConfig> columnsConfig) {
    List<DataGeneratorColumn> updatedColumns = new ArrayList<>();
    for (DataGeneratorColumn col : existingTable.columns()) {
      if (!columnsConfig.containsKey(col.name())) {
        updatedColumns.add(col);
        continue;
      }
      SchemaConfig.ColumnConfig colConfig = columnsConfig.get(col.name());
      DataGeneratorColumn.Builder colBuilder = col.toBuilder();

      if (colConfig.getFakerExpression() != null) {
        colBuilder.fakerExpression(colConfig.getFakerExpression());
      }
      if (colConfig.getSkip() != null) {
        boolean skip = colConfig.getSkip();
        if (skip && col.isPrimaryKey()) {
          throw new IllegalArgumentException(
              "Cannot skip primary-key column '"
                  + col.name()
                  + "' in table '"
                  + existingTable.name()
                  + "': PK values are required for state-keying and lifecycle events.");
        }
        colBuilder.isSkipped(skip);
      }
      updatedColumns.add(colBuilder.build());
    }
    return ImmutableList.copyOf(updatedColumns);
  }

  private ImmutableList<DataGeneratorForeignKey> mergeForeignKeys(
      DataGeneratorTable existingTable, List<SchemaConfig.ForeignKeyConfig> fkConfigs) {
    // Use LinkedHashMap to preserve the discovered schema's physical foreign key insertion order.
    // This guarantees that downstream DFS graph traversals remain
    // 100% deterministic and reproducible across cluster worker node restarts and unit tests.
    LinkedHashMap<String, DataGeneratorForeignKey> mergedFksByName = new LinkedHashMap<>();
    for (DataGeneratorForeignKey fk : existingTable.foreignKeys()) {
      mergedFksByName.put(fk.name(), fk);
    }

    for (SchemaConfig.ForeignKeyConfig fkConfig : fkConfigs) {
      String fkName = fkConfig.getName();
      String referencedTable = fkConfig.getReferencedTable();
      List<String> keyColumns = fkConfig.getKeyColumns();
      List<String> referencedColumns = fkConfig.getReferencedColumns();

      DataGeneratorForeignKey configuredFk =
          DataGeneratorForeignKey.builder()
              .name(fkName)
              .referencedTable(referencedTable)
              .keyColumns(
                  keyColumns != null ? ImmutableList.copyOf(keyColumns) : ImmutableList.of())
              .referencedColumns(
                  referencedColumns != null
                      ? ImmutableList.copyOf(referencedColumns)
                      : ImmutableList.of())
              .build();

      DataGeneratorForeignKey discoveredFk = mergedFksByName.get(fkName);
      if (discoveredFk != null && !fkEquivalent(discoveredFk, configuredFk)) {
        throw new IllegalArgumentException(
            "Foreign key '"
                + fkName
                + "' on table '"
                + existingTable.name()
                + "' conflicts with the discovered definition. discovered=[refTable="
                + discoveredFk.referencedTable()
                + ", keyColumns="
                + discoveredFk.keyColumns()
                + ", referencedColumns="
                + discoveredFk.referencedColumns()
                + "], config=[refTable="
                + configuredFk.referencedTable()
                + ", keyColumns="
                + configuredFk.keyColumns()
                + ", referencedColumns="
                + configuredFk.referencedColumns()
                + "]. Align the config with the source schema or rename the FK.");
      }
      mergedFksByName.put(fkName, configuredFk);
    }
    return ImmutableList.copyOf(mergedFksByName.values());
  }

  private static boolean fkEquivalent(DataGeneratorForeignKey a, DataGeneratorForeignKey b) {
    return a.referencedTable().equals(b.referencedTable())
        && a.keyColumns().equals(b.keyColumns())
        && a.referencedColumns().equals(b.referencedColumns());
  }
}
